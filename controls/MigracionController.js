'use strict';
const { client, databaseId } = require('../routes/index');
const Sequelize = require('sequelize');
const { v4: uuidv4 } = require('uuid');
const { Op } = require("sequelize");
const models = require('../models');

// Umbral para valores válidos
const VALOR_UMBRAL = 1e6;

const medidaMapeo = {
  "EMA": {
    "Temperatura": "Temperatura",
    "Presion": "Presión atmosférica",
    "Humedad": "Humedad",
    "Lluvia": "Lluvia"
  },
  "EHA": {
    "Nivel_de_agua": "Nivel de agua",
    "Carga_H": "Carga H",
    "Distancia_Hs": "Distancia H"
  }
};

class MigracionController {
  constructor() {
    this.database = client.database(databaseId);
  }

  esValorValido(valor) {
    return valor !== null && !isNaN(valor) && Math.abs(valor) < VALOR_UMBRAL;
  }

  async obtenerFechasParaMigrar() {
    const fechaActual = new Date();

    // Obtener la última fecha migrada
    const ultimaMedicion = await models.Medicion.findOne({
      order: [['fecha_local', 'DESC']],
    });

    // Si no hay registros, empezar desde el día anterior a la fecha actual
    const fechaInicio = ultimaMedicion
      ? ultimaMedicion.fecha_local
      : new Date(fechaActual.setDate(fechaActual.getDate() - 1));

    // Ajustar hora a medianoche y verificar si es un día completo
    fechaInicio.setHours(0, 0, 0, 0);

    if (fechaInicio.toDateString() === fechaActual.toDateString()) {
      fechaInicio.setDate(fechaActual.getDate() - 1);
    }

    return {
      inicio: fechaInicio.toISOString().split('T')[0],
      fin: new Date(fechaActual.setHours(0, 0, 0, 0)).toISOString().split('T')[0],
    };
  }

  async migrarYCalcularMedicionesParaTodosLosDias() {
    const contenedores = ["EMA", "EHA"];
    const { inicio, fin } = await this.obtenerFechasParaMigrar();
    const fechasParaMigrar = this.generarRangoFechas(inicio, fin);

    if (fechasParaMigrar.length === 0) {
      console.log("No hay días completos para migrar.");
      return;
    }

    try {
      for (const fecha of fechasParaMigrar) {
        console.log(`Procesando registros para la fecha: ${fecha}`);
        for (const contenedor of contenedores) {
          const container = this.database.container(contenedor);
          const mapeo = medidaMapeo[contenedor];
          const items = await this.obtenerDatosCosmosConFiltro(container, fecha);
          const estaciones = this.agruparDatosPorEstacion(items, mapeo);
          await this.procesarEstaciones(estaciones, fecha);
        }
      }
      console.log('Migración y cálculos completados para todos los días completos.');
    } catch (error) {
      console.error("Error en migración y cálculo:", error);
    }
  }

  generarRangoFechas(inicio, fin) {
    const fechas = [];
    let fechaActual = new Date(inicio);
    const fechaFin = new Date(fin);

    while (fechaActual < fechaFin) {
      fechas.push(new Date(fechaActual).toISOString().split('T')[0]);
      fechaActual.setDate(fechaActual.getDate() + 1);
    }
    return fechas;
  }

  async obtenerDatosCosmosConFiltro(container, fecha) {
    try {
      const query = {
        query: "SELECT * FROM c WHERE STARTSWITH(c['Fecha_local_UTC-5'], @fecha)",
        parameters: [{ name: "@fecha", value: fecha }]
      };
      const { resources: items } = await container.items.query(query).fetchAll();
      return items;
    } catch (error) {
      console.error(`Error al obtener datos de Cosmos para el contenedor ${container.id}:`, error);
      return [];
    }
  }

  agruparDatosPorEstacion(items, mapeo) {
    const estaciones = {};
    items.forEach(item => {
      const deviceId = item.deviceId;
      if (!deviceId) {
        console.warn("Registro sin `deviceId` encontrado:", item);
        return;
      }

      for (const [campo, valor] of Object.entries(item)) {
        if (campo === "deviceId") continue;

        const medidaNombre = mapeo[campo];
        if (!medidaNombre) continue;

        if (!estaciones[deviceId]) estaciones[deviceId] = {};
        if (!estaciones[deviceId][medidaNombre]) estaciones[deviceId][medidaNombre] = [];

        if (this.esValorValido(valor)) {
          estaciones[deviceId][medidaNombre].push(valor);
        } else {
          console.warn(`Valor anómalo detectado y excluido: ${valor} en ${medidaNombre} para deviceId ${deviceId}`);
        }
      }
    });
    return estaciones;
  }

  async procesarEstaciones(estaciones, fecha) {
    for (const [deviceId, medidas] of Object.entries(estaciones)) {
      const estacion = await models.estacion.findOne({ where: { id_dispositivo: deviceId } });

      if (!estacion) {
        console.warn(`Estación no encontrada para deviceId ${deviceId}`);
        continue;
      }

      for (const [medidaNombre, valores] of Object.entries(medidas)) {
        const medida = await models.tipo_medida.findOne({ where: { nombre: medidaNombre } });
        if (!medida) {
          console.warn(`Medida no encontrada para ${medidaNombre}`);
          continue;
        }

        const medidaEstaciones = await models.medida_estacion.findAll({
          where: {
            id_estacion: estacion.id,
            id_medida_operacion: {
              [Op.in]: Sequelize.literal(`(SELECT id FROM medida_operacion WHERE id_tipo_medida = ${medida.id})`)
            }
          }
        });

        console.log(`Valores para la medida ${medidaNombre} en la estación ${deviceId}:`, valores);
        await this.calcularYGuardarMediciones(medidaEstaciones, valores, fecha);
      }
    }
  }

  async calcularYGuardarMediciones(medidaEstaciones, valores, fecha) {
    for (const medidaEstacion of medidaEstaciones) {
      const operacion = await models.medida_operacion.findByPk(medidaEstacion.id_medida_operacion, {
        include: [{ model: models.tipo_operacion }]
      });

      if (!operacion) {
        console.warn(`Operación no encontrada para medidaEstacion id ${medidaEstacion.id}`);
        continue;
      }

      let valorCalculado = 0;

      // Filtrar solo valores válidos y depurar cada valor individual
      const valoresValidos = valores.filter(v => this.esValorValido(v));

      console.log(`Valores válidos (depurados) para medida ${medidaEstacion.id}:`, valoresValidos);

      // Calcular según el tipo de operación
      switch (operacion.tipo_operacion.operacion) {
        case 'PROMEDIO':
          if (valoresValidos.length > 0) {
            valorCalculado = valoresValidos.reduce((a, b) => a + b, 0) / valoresValidos.length;
          }
          break;

        case 'MAX':
          valorCalculado = valoresValidos.length > 0 ? Math.max(...valoresValidos) : 0;
          break;

        case 'MIN':
          valorCalculado = valoresValidos.length > 0 ? Math.min(...valoresValidos) : 0;
          break;

        case 'SUMA':
          valorCalculado = valoresValidos.reduce((a, b) => a + b, 0);
          console.log(`Suma calculada para medida ${medidaEstacion.id}: ${valorCalculado}`);
          break;

        default:
          console.warn(`Operación desconocida para medidaEstacion id ${medidaEstacion.id}: ${operacion.tipo_operacion.operacion}`);
          continue;
      }

      console.log(`Resultado final para operación ${operacion.tipo_operacion.operacion} en medida ${medidaEstacion.id}:`, valorCalculado);

      // Guardar solo si el valor calculado es válido
      if (this.esValorValido(valorCalculado)) {
        try {
          await models.medicion.create({
            fecha_local: new Date(`${fecha}T00:00:00`),
            valor: valorCalculado.toFixed(2),
            external_id: uuidv4(),
            estado: 1,
            id_medida_estacion: medidaEstacion.id
          });
          console.log(`Registro insertado para medidaEstacion id ${medidaEstacion.id}`);
        } catch (error) {
          console.error(`Error al insertar el registro en la tabla 'medicion':`, error);
        }
      } else {
        console.warn(`Valor calculado fuera de rango, no se guarda: ${valorCalculado} para operación ${operacion.tipo_operacion.operacion}`);
      }
    }
  }
}

async function obtenerFechasParaMigrar() {
  // Obtener la fecha actual
  const fechaActual = new Date();
  
  // Obtener el último registro de medición en la BD para saber hasta qué fecha se ha migrado
  const ultimaMedicion = await models.Medicion.findOne({
    order: [['fecha_local', 'DESC']], // Ordenamos por fecha de manera descendente
  });

  // Si no hay registros en la base de datos, empezar desde el día anterior
  const fechaInicio = ultimaMedicion ? ultimaMedicion.fecha_local : new Date(fechaActual.setDate(fechaActual.getDate() - 1));
  
  // Formatear la fecha de inicio (debe ser a medianoche)
  fechaInicio.setHours(0, 0, 0, 0);

  // Verificar si la fecha de inicio es la misma que la fecha actual (si el servidor se inicia a mediodía, no sincronizamos el día actual)
  if (fechaActual.getDate() === fechaInicio.getDate()) {
    // Si estamos en el mismo día, evitamos migrar el día incompleto
    fechaInicio.setDate(fechaActual.getDate() - 1);
  }

  // Devolver las fechas que deben migrarse (de la fecha siguiente al último registro hasta la fecha de ayer)
  return {
    inicio: fechaInicio.toISOString().split('T')[0], // Devolvemos solo la fecha (sin la hora)
    fin: new Date(fechaActual.setHours(0, 0, 0, 0)).toISOString().split('T')[0], // Fecha de ayer a medianoche
  };
}

module.exports = MigracionController;

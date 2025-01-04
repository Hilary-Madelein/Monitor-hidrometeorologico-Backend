const { client, databaseId, getAllContainers } = require('../routes/index');
require('dotenv').config();

const validarParametros = ({ escalaDeTiempo, mes, anio, fechaInicio, fechaFin }) => {
    if (!escalaDeTiempo && (!mes || !anio) && (!fechaInicio || !fechaFin)) {
        throw new Error('Debe proporcionar una escala de tiempo válida, un mes/año o un rango de fechas.');
    }
};

const calcularFechas = (escalaDeTiempo, mes, anio, fechaInicio, fechaFin, ultimaFecha) => {
    let fechaInicioISO, fechaFinISO;

    if (escalaDeTiempo) {
        switch (escalaDeTiempo) {
            case '15min': fechaInicioISO = new Date(ultimaFecha.getTime() - 15 * 60000).toISOString(); break;
            case '30min': fechaInicioISO = new Date(ultimaFecha.getTime() - 30 * 60000).toISOString(); break;
            case 'hora': fechaInicioISO = new Date(ultimaFecha.getTime() - 60 * 60000).toISOString(); break;
            case 'diaria': fechaInicioISO = new Date(ultimaFecha.getFullYear(), ultimaFecha.getMonth(), ultimaFecha.getDate()).toISOString(); break;
            default: throw new Error('Escala de tiempo inválida.');
        }
        fechaFinISO = ultimaFecha.toISOString();
    } else if (mes && anio) {
        fechaInicioISO = new Date(anio, mes - 1, 1).toISOString();
        fechaFinISO = new Date(anio, mes, 0).toISOString();
    } else if (fechaInicio && fechaFin) {
        fechaInicioISO = new Date(fechaInicio).toISOString();
        fechaFinISO = new Date(fechaFin).toISOString();
        if (isNaN(Date.parse(fechaInicioISO)) || isNaN(Date.parse(fechaFinISO))) {
            throw new Error('Fechas inválidas.');
        }
    }

    return { fechaInicioISO, fechaFinISO };
};

const filtrarDatosValidos = (items, limitesEspecificos, umbralGeneral) => {
    const esValorValido = (campo, valor) => {
        if (limitesEspecificos[campo]) {
            const { min, max } = limitesEspecificos[campo];
            return valor !== null && !isNaN(valor) && valor >= min && valor <= max;
        }
        return valor !== null && !isNaN(valor) && Math.abs(valor) < umbralGeneral;
    };

    return items.filter(item => 
        Object.keys(item).every(campo => esValorValido(campo, item[campo]))
    );
};

class MedidaController {
    async getUltimasTenMedidas(req, res) {
        try {
            const containers = await getAllContainers();

            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }

            const containerId = containers[1];
            const database = client.database(databaseId);
            const container = database.container(containerId);

            const query = {
                query: "SELECT * FROM c ORDER BY c._ts DESC OFFSET 0 LIMIT 10"
            };

            // Ejecutar la consulta
            const { resources: items } = await container.items.query(query).fetchAll();

            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: items
            });
        } catch (error) {
            console.error('Error en getUltimasMedidas:', error);
            res.status(500).json({
                msg: 'Se produjo un error al listar las últimas medidas',
                code: 500,
                info: error.message
            });
        }
    }

    async getUltimaMedicion(req, res) {
        try {
            const containers = await getAllContainers();

            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }

            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);

            const query = {
                query: "SELECT * FROM c ORDER BY c._ts DESC OFFSET 0 LIMIT 1"
            };

            // Ejecutar la consulta
            const { resources: items } = await container.items.query(query).fetchAll();

            if (items.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron registros',
                    code: 404
                });
            }

            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: items[0]
            });
        } catch (error) {
            console.error('Error en getUltimaMedicion:', error);
            res.status(500).json({
                msg: 'Se produjo un error al listar la última medición',
                code: 500,
                info: error.message
            });
        }
    }

    //METODO PARA OBETENER MEDIDAS POR MES
    async getMedidasPromediadasPorDia(req, res) {
        try {
            const { fechaInicio, fechaFin } = req.body;
    
            if (!fechaInicio || !fechaFin) {
                return res.status(400).json({
                    msg: 'Debe proporcionar un rango de fechas válido (fechaInicio y fechaFin).',
                    code: 400
                });
            }
    
            const inicio = new Date(fechaInicio);
            const fin = new Date(fechaFin);
    
            if (isNaN(inicio.getTime()) || isNaN(fin.getTime())) {
                return res.status(400).json({
                    msg: 'El formato de las fechas es inválido. Deben ser fechas válidas en formato ISO.',
                    code: 400
                });
            }
    
            if (inicio > fin) {
                return res.status(400).json({
                    msg: 'La fecha de inicio no puede ser posterior a la fecha de fin.',
                    code: 400
                });
            }
    
            const containers = await getAllContainers();
            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }
    
            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            const medidasConfig = {
                Temperatura: "AVG",
                Humedad: "AVG",
                Presion: "AVG",
                Lluvia: "SUM",
                Nivel_de_agua: "AVG",
                Carga_H: "AVG",
                Distancia_Hs: "AVG"
            };
    
            const selectClauses = ['c["Fecha_local_UTC-5"]'];
            Object.keys(medidasConfig).forEach(medida => {
                const operacion = medidasConfig[medida];
                const campoAlias = `${operacion.toLowerCase()}_${medida}`;
                selectClauses.push(`${operacion}(c.${medida}) AS ${campoAlias}`);
            });
    
            const query = {
                query: `SELECT ${selectClauses.join(', ')}
                        FROM c
                        WHERE c["Fecha_local_UTC-5"] >= @inicio 
                          AND c["Fecha_local_UTC-5"] <= @fin
                        GROUP BY c["Fecha_local_UTC-5"]
                        ORDER BY c["Fecha_local_UTC-5"] DESC`,
                parameters: [
                    { name: "@inicio", value: fechaInicio },
                    { name: "@fin", value: fechaFin }
                ]
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos en el rango de fechas proporcionado`,
                    code: 404
                });
            }
    
            const umbralMaximo = 1e6;
            const esValorValido = (valor) => valor !== null && !isNaN(valor) && Math.abs(valor) < umbralMaximo;
    
            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = new Date(item["Fecha_local_UTC-5"]);
                const dia = `${fechaUTC.getUTCFullYear()}-${(fechaUTC.getUTCMonth() + 1).toString().padStart(2, '0')}-${fechaUTC.getUTCDate().toString().padStart(2, '0')}`;
    
                if (!acc[dia]) {
                    acc[dia] = { medidas: {}, count: 0 };
                }
    
                Object.keys(medidasConfig).forEach(medida => {
                    const operacion = medidasConfig[medida];
                    const valor = item[`${operacion.toLowerCase()}_${medida}`];
                    if (esValorValido(valor)) {
                        if (!acc[dia].medidas[medida]) {
                            acc[dia].medidas[medida] = { suma: 0, count: 0 };
                        }
                        acc[dia].medidas[medida].suma += valor;
                        acc[dia].medidas[medida].count++;
                    }
                });
    
                acc[dia].count++;
                return acc;
            }, {});
    
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const medidasPromedio = {};
    
                Object.keys(medidasConfig).forEach(medida => {
                    const operacion = medidasConfig[medida];
                    if (medidasDia[medida]) {
                        const suma = medidasDia[medida].suma;
                        const count = medidasDia[medida].count;
                        medidasPromedio[medida] = operacion === "AVG" ? suma / count : suma;
                    }
                });
    
                return {
                    dia,
                    medidas: medidasPromedio
                };
            });
    
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: medidasPromediadasPorDia
            });
        } catch (error) {
            console.error('Error en getMedidasPromediadasPorDia:', error);
            res.status(500).json({
                msg: 'Se produjo un error al listar las medidas promediadas por día',
                code: 500,
                info: error.message
            });
        }
    }   
  
    /*async getMedidasPromediadasPorMes(req, res) {
        try {
            const { mes, anio } = req.body;
    
            if (!mes || !anio) {
                return res.status(400).json({
                    msg: 'Debe proporcionar un mes y año válidos.',
                    code: 400
                });
            }
    
            const mesInt = parseInt(mes);
            const anioInt = parseInt(anio);
    
            if (isNaN(mesInt) || isNaN(anioInt) || mesInt < 1 || mesInt > 12) {
                return res.status(400).json({
                    msg: 'El mes debe estar entre 1 y 12 y el año debe ser un número válido.',
                    code: 400
                });
            }
    
            const fechaInicio = new Date(anioInt, mesInt - 1, 1);
            const fechaFin = new Date(anioInt, mesInt, 0);
            const containers = await getAllContainers();
    
            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }
    
            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            const medidasConfig = {
                Temperatura: "AVG",
                Humedad: "AVG",
                Presion: "AVG",
                Lluvia: "SUM",
                Nivel_de_agua: "AVG",
                Carga_H: "AVG",
                Distancia_Hs: "AVG"
            };
    
            const selectClauses = ['c["Fecha_local_UTC-5"]'];
            Object.keys(medidasConfig).forEach(medida => {
                const operacion = medidasConfig[medida];
                const campoAlias = `${operacion.toLowerCase()}_${medida}`;
                selectClauses.push(`${operacion}(c.${medida}) AS ${campoAlias}`);
            });
    
            const query = {
                query: `SELECT ${selectClauses.join(', ')}
                        FROM c
                        WHERE c["Fecha_local_UTC-5"] >= @inicio
                          AND c["Fecha_local_UTC-5"] <= @fin
                        GROUP BY c["Fecha_local_UTC-5"]
                        ORDER BY c["Fecha_local_UTC-5"] DESC`,
                parameters: [
                    { name: "@inicio", value: fechaInicio.toISOString() },
                    { name: "@fin", value: fechaFin.toISOString() }
                ]
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (!items || items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos para el mes proporcionado`,
                    code: 404
                });
            }
    
            const umbralMaximo = 1e6;
            const esValorValido = (valor) => valor !== null && !isNaN(valor) && Math.abs(valor) < umbralMaximo;
    
            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;
    
                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 };
                    }
    
                    Object.keys(medidasConfig).forEach(medida => {
                        const operacion = medidasConfig[medida];
                        const valor = item[`${operacion.toLowerCase()}_${medida}`];
                        if (esValorValido(valor)) {
                            if (!acc[dia].medidas[medida]) {
                                acc[dia].medidas[medida] = { suma: 0, count: 0 };
                            }
                            acc[dia].medidas[medida].suma += valor;
                            acc[dia].medidas[medida].count++;
                        }
                    });
    
                    acc[dia].count++;
                }
                return acc;
            }, {});
    
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const medidasPromedio = {};
    
                Object.keys(medidasConfig).forEach(medida => {
                    const operacion = medidasConfig[medida];
                    if (medidasDia[medida]) {
                        const suma = medidasDia[medida].suma;
                        const count = medidasDia[medida].count;
                        medidasPromedio[medida] = operacion === "AVG" ? suma / count : suma;
                    }
                });
    
                return {
                    dia,
                    medidas: medidasPromedio
                };
            });
    
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: medidasPromediadasPorDia
            });
        } catch (error) {
            console.error('Error en getMedidasPromediadasPorMes:', error);
            res.status(500).json({
                msg: 'Se produjo un error al listar las medidas promediadas por día',
                code: 500,
                info: error.message
            });
        }
    }*/

        
        //REVISAR LUEGO
        async getDatosClimaticosPorEscala(req, res) {
            try {
                const { escalaDeTiempo, mes, anio, fechaInicio, fechaFin } = req.body;
        
                // Validar parámetros
                validarParametros({ escalaDeTiempo, mes, anio, fechaInicio, fechaFin });
        
                const containers = await getAllContainers();
                if (!containers || containers.length === 0) {
                    return res.status(404).json({ msg: 'No se encontraron estaciones registradas', code: 404 });
                }
        
                const containerId = containers[0];
                const database = client.database(databaseId);
                const container = database.container(containerId);
        
                const ultimoRegistroQuery = {
                    query: `SELECT TOP 1 c["Fecha_local_UTC-5"] FROM c ORDER BY c["Fecha_local_UTC-5"] DESC`
                };
                const { resources: ultimoRegistro } = await container.items.query(ultimoRegistroQuery).fetchAll();
                if (ultimoRegistro.length === 0) {
                    return res.status(404).json({ msg: 'No se encontró ningún registro en la base de datos', code: 404 });
                }
        
                const ultimaFecha = new Date(ultimoRegistro[0]["Fecha_local_UTC-5"]);
                const { fechaInicioISO, fechaFinISO } = calcularFechas(escalaDeTiempo, mes, anio, fechaInicio, fechaFin, ultimaFecha);
        
                const query = {
                    query: `SELECT 
                                c.Temperatura,
                                c.Humedad,
                                c.Presion,
                                c.Lluvia,
                                c.Nivel_de_agua,
                                c.Carga_H,
                                c.Distancia_Hs
                            FROM c
                            WHERE c["Fecha_local_UTC-5"] >= @inicio
                              AND c["Fecha_local_UTC-5"] <= @fin`,
                    parameters: [
                        { name: "@inicio", value: fechaInicioISO },
                        { name: "@fin", value: fechaFinISO }
                    ]
                };
        
                const { resources: items } = await container.items.query(query).fetchAll();
                if (items.length === 0) {
                    return res.status(404).json({ msg: 'No se encontraron datos.', code: 404 });
                }
        
                const umbralGeneral = 1e6;
                const limitesEspecificos = { Humedad: { min: 0, max: 100 } };
                const datosValidos = filtrarDatosValidos(items, limitesEspecificos, umbralGeneral);
        
                if (datosValidos.length === 0) {
                    return res.status(404).json({ msg: 'No se encontraron datos válidos.', code: 404 });
                }
        
                const medidasConfig = {
                    Temperatura: ["AVG", "MAX", "MIN"],
                    Humedad: ["AVG"],
                    Presion: ["AVG"],
                    Lluvia: ["SUM"],
                    Nivel_de_agua: ["AVG", "MAX"],
                    Carga_H: ["AVG"],
                    Distancia_Hs: ["MIN", "MAX"]
                };
        
                const resultados = {};
                Object.keys(medidasConfig).forEach(campo => {
                    const valores = datosValidos.map(d => d[campo]).filter(v => v !== null && v !== undefined);
                    if (valores.length > 0) {
                        medidasConfig[campo].forEach(operacion => {
                            switch (operacion) {
                                case 'AVG':
                                    resultados[`avg_${campo}`] = valores.reduce((a, b) => a + b, 0) / valores.length;
                                    break;
                                case 'MAX':
                                    resultados[`max_${campo}`] = Math.max(...valores);
                                    break;
                                case 'MIN':
                                    resultados[`min_${campo}`] = Math.min(...valores);
                                    break;
                                case 'SUM':
                                    resultados[`sum_${campo}`] = valores.reduce((a, b) => a + b, 0);
                                    break;
                            }
                        });
                    }
                });
        
                res.status(200).json({ msg: 'OK!', code: 200, container: containerId, info: resultados });
        
            } catch (error) {
                console.error('Error en getDatosClimaticosPorEscala:', error);
                res.status(500).json({ msg: 'Error al obtener datos climáticos', code: 500, info: error.message });
            }
        }
         
     
    
    async getAllDatosClimaticosPorEscala(req, res) {
        try {
            const { escalaDeTiempo } = req.body;
    
            // Validación inicial
            if (!escalaDeTiempo) {
                return res.status(400).json({
                    msg: 'Debe proporcionar una escala de tiempo válida (15min, 30min, hora, diaria).',
                    code: 400
                });
            }
    
            const containers = await getAllContainers();
            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }
    
            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            // Calcular el rango de fechas basado en la escala de tiempo
            const ahora = new Date();
            let fechaInicio;
    
            switch (escalaDeTiempo) {
                case '15min':
                    fechaInicio = new Date(ahora.getTime() - 15 * 60000 - 300 * 60000);
                    break;
                case '30min':
                    fechaInicio = new Date(ahora.getTime() - 30 * 60000 - 300 * 60000);
                    break;
                case 'hora':
                    fechaInicio = new Date(ahora.getTime() - 60 * 60000 - 300 * 60000);
                    break;
                case 'diaria':
                    fechaInicio = new Date(ahora.getFullYear(), ahora.getMonth(), ahora.getDate());
                    fechaInicio.setTime(fechaInicio.getTime() - 300 * 60000);
                    break;
                default:
                    return res.status(400).json({
                        msg: 'Escala de tiempo inválida. Use 15min, 30min, hora, diaria.',
                        code: 400
                    });
            }
    
            const fechaFin = new Date(ahora.getTime() - 300 * 60000);
            const fechaInicioISO = fechaInicio.toISOString();
            const fechaFinISO = fechaFin.toISOString();
    
            // Consulta para obtener todos los registros en el rango de tiempo
            const query = {
                query: `SELECT 
                            c["Fecha_local_UTC-5"],
                            c.Temperatura,
                            c.Humedad,
                            c.Presion,
                            c.Lluvia,
                            c.Nivel_de_agua,
                            c.Carga_H,
                            c.Distancia_Hs
                        FROM c
                        WHERE c["Fecha_local_UTC-5"] >= @inicio
                          AND c["Fecha_local_UTC-5"] <= @fin
                        ORDER BY c["Fecha_local_UTC-5"] ASC`,
                parameters: [
                    { name: "@inicio", value: fechaInicioISO },
                    { name: "@fin", value: fechaFinISO }
                ]
            };
    
            console.log("Consulta generada:", query.query);
            console.log("Parámetros:", query.parameters);
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (!items || items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos climáticos para el rango de tiempo proporcionado`,
                    code: 404
                });
            }
    
            // Devolver todos los registros obtenidos
            return res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: items // Aquí se devuelve directamente la lista de registros
            });
    
        } catch (error) {
            console.error('Error detallado:', JSON.stringify(error, null, 2));
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos.',
                code: 500,
                info: error.message
            });
        }
    }
    
    
    
       
    
    /*async getDatosClimaticosPorEscalaMensual(req, res) {
        try {
            const { escalaDeTiempo } = req.body;
    
            if (!escalaDeTiempo || escalaDeTiempo !== 'mensual') {
                return res.status(400).json({
                    msg: 'Debe proporcionar una escala de tiempo válida. En este caso, solo se acepta "mensual".',
                    code: 400
                });
            }
    
            const containers = await getAllContainers();
            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }
    
            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            const medidasConfig = {
                Temperatura: ["AVG"],
                Humedad: ["AVG"],
                Presion: ["AVG"],
                Lluvia: ["SUM"],
                Nivel_de_agua: ["AVG", "MAX"],
                Carga_H: ["AVG"],
                Distancia_Hs: ["MIN", "MAX"]
            };
    
            const selectClauses = ['c["Fecha_local_UTC-5"]'];
            const medidasDisponibles = new Set();
    
            for (const [campo, operaciones] of Object.entries(medidasConfig)) {
                operaciones.forEach(op => {
                    const campoAlias = `${op.toLowerCase()}_${campo}`;
                    selectClauses.push(`${op}(c.${campo}) AS ${campoAlias}`);
                });
            }
    
            const query = {
                query: `SELECT ${selectClauses.join(', ')}
                        FROM c
                        GROUP BY c["Fecha_local_UTC-5"]
                        ORDER BY c["Fecha_local_UTC-5"] DESC`
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
            if (items.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron datos climáticos',
                    code: 404
                });
            }
    
            const datosPorMes = {};
            const limites = {
                Temperatura: { min: -50, max: 100 },
                Humedad: { min: 0, max: 100 },
                Presion: { min: 50, max: 2100 },
                Lluvia: { min: 0, max: 2000 },
                Nivel_de_agua: { min: 0, max: 100 },
                Carga_H: { min: 0, max: 100 },
                Distancia_Hs: { min: 0, max: 1000 }
            };
    
            const esValorValido = (valor, min, max) => valor !== null && valor !== undefined && valor >= min && valor <= max;
    
            items.forEach(item => {
                const fecha = new Date(item["Fecha_local_UTC-5"]);
                const mes = `${fecha.getFullYear()}-${String(fecha.getMonth() + 1).padStart(2, '0')}`;
    
                if (!datosPorMes[mes]) {
                    datosPorMes[mes] = { totalDatos: 0 };
                }
    
                Object.keys(medidasConfig).forEach(medida => {
                    if (item[`avg_${medida}`] !== undefined || item[`sum_${medida}`] !== undefined) {
                        const valores = medidasConfig[medida].map(op => item[`${op.toLowerCase()}_${medida}`]);
                        const { min, max } = limites[medida] || { min: -Infinity, max: Infinity };
    
                        valores.forEach((valor, index) => {
                            if (esValorValido(valor, min, max)) {
                                datosPorMes[mes][`${medida}Total`] = (datosPorMes[mes][`${medida}Total`] || 0) + valor;
                                medidasDisponibles.add(medida);
                            }
                        });
                    }
                });
    
                datosPorMes[mes].totalDatos++;
            });
    
            const resultadosPorMes = Object.keys(datosPorMes).map(mes => {
                const datosMes = datosPorMes[mes];
                const medidas = {};
    
                medidasDisponibles.forEach(medida => {
                    if (medidasConfig[medida].includes("SUM")) {
                        medidas[medida] = datosMes[`${medida}Total`] || 0;
                    } else {
                        medidas[medida] = datosMes[`${medida}Total`] / datosMes.totalDatos || 0;
                    }
                });
    
                return {
                    mes,
                    medidas
                };
            });
    
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: resultadosPorMes
            });
    
        } catch (error) {
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala mensual',
                code: 500,
                info: error.message
            });
        }
    }*/   
    
    
    /*async getDatosClimaticosMensual(req, res) {
        try {
            const { escalaDeTiempo } = req.body;
    
            if (!escalaDeTiempo || escalaDeTiempo !== 'mensual') {
                return res.status(400).json({
                    msg: 'Debe proporcionar una escala de tiempo válida. En este caso, solo se acepta "mensual".',
                    code: 400
                });
            }
    
            const containers = await getAllContainers();
    
            if (!containers || containers.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron estaciones registradas',
                    code: 404
                });
            }
    
            const containerId = containers[0];
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            // Configuración de medidas y operaciones
            const medidasConfig = {
                Temperatura: ["AVG", "MAX", "MIN"],
                Humedad: ["AVG"],
                Presion: ["AVG"],
                Lluvia: ["SUM"],
                Nivel_de_agua: ["AVG", "MAX"],
                Carga_H: ["AVG"],
                Distancia_Hs: ["MIN", "MAX"]
            };
    
            const selectClauses = ['c["Fecha_local_UTC-5"] AS fecha'];
            for (const [campo, operaciones] of Object.entries(medidasConfig)) {
                operaciones.forEach(op => {
                    const campoAlias = `${op.toLowerCase()}_${campo}`;
                    selectClauses.push(`${op}(c.${campo}) AS ${campoAlias}`);
                });
            }
    
            const query = {
                query: `SELECT ${selectClauses.join(', ')}
                        FROM c
                        GROUP BY c["Fecha_local_UTC-5"]
                        ORDER BY c["Fecha_local_UTC-5"] ASC`
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron datos climáticos',
                    code: 404
                });
            }
    
            const datosPorMes = {};
            const umbralMaximo = 1e6; // Umbral para valores fuera de rango
    
            const esValorValido = valor => valor !== null && !isNaN(valor) && Math.abs(valor) < umbralMaximo;
    
            items.forEach(item => {
                const fecha = new Date(item.fecha);
                const mes = `${fecha.getFullYear()}-${String(fecha.getMonth() + 1).padStart(2, '0')}`;
    
                if (!datosPorMes[mes]) {
                    datosPorMes[mes] = {};
                }
    
                for (const campo in medidasConfig) {
                    medidasConfig[campo].forEach(op => {
                        const alias = `${op.toLowerCase()}_${campo}`;
                        if (esValorValido(item[alias])) {
                            if (op === "SUM") {
                                datosPorMes[mes][alias] = (datosPorMes[mes][alias] || 0) + item[alias];
                            } else if (op === "MAX") {
                                datosPorMes[mes][alias] = datosPorMes[mes][alias] ? Math.max(datosPorMes[mes][alias], item[alias]) : item[alias];
                            } else if (op === "MIN") {
                                datosPorMes[mes][alias] = datosPorMes[mes][alias] ? Math.min(datosPorMes[mes][alias], item[alias]) : item[alias];
                            } else if (op === "AVG") {
                                datosPorMes[mes][alias] = datosPorMes[mes][alias] || { sum: 0, count: 0 };
                                datosPorMes[mes][alias].sum += item[alias];
                                datosPorMes[mes][alias].count += 1;
                            }
                        }
                    });
                }
            });
    
            const resultadosPorMes = Object.keys(datosPorMes).map(mes => {
                const medidas = {};
                for (const alias in datosPorMes[mes]) {
                    if (typeof datosPorMes[mes][alias] === "object" && datosPorMes[mes][alias].count) {
                        medidas[alias] = datosPorMes[mes][alias].sum / datosPorMes[mes][alias].count;
                    } else {
                        medidas[alias] = datosPorMes[mes][alias];
                    }
                }
                return { mes, ...medidas };
            });
    
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: resultadosPorMes
            });
    
        } catch (error) {
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala mensual',
                code: 500,
                info: error.message
            });
        }
    }*/
             
}

module.exports = MedidaController;

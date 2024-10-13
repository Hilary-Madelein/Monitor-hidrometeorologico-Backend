const { client, databaseId, getAllContainers } = require('../routes/index');
require('dotenv').config();

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

            const containerId = containers[0];
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
    
            // Validar que las fechas existan y sean válidas
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
    
            // Modificar la consulta para calcular promedios directamente en la BD
            const query = {
                query: `
                    SELECT 
                        c["Fecha_local_UTC-5"],
                        c.Temperatura,
                        c.Humedad,
                        c.Presion,
                        c.Lluvia
                    FROM c
                    WHERE c["Fecha_local_UTC-5"] >= @inicio 
                      AND c["Fecha_local_UTC-5"] <= @fin
                      AND IS_NUMBER(c.Temperatura) AND c.Temperatura >= 1e-5 AND c.Temperatura <= 1e6
                      AND IS_NUMBER(c.Humedad) AND c.Humedad >= 1e-5 AND c.Humedad <= 1e6
                      AND IS_NUMBER(c.Presion) AND c.Presion >= 1e-5 AND c.Presion <= 1e6
                      AND IS_NUMBER(c.Lluvia) AND c.Lluvia >= 0 AND c.Lluvia <= 1e6
                    ORDER BY c["Fecha_local_UTC-5"] ASC`,
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
    
            // Estructurar las medidas promediadas por día
            const medidasAgrupadasPorDia = {};
    
            items.forEach(item => {
                const fechaUTC = new Date(item["Fecha_local_UTC-5"]);
                const dia = `${fechaUTC.getUTCFullYear()}-${(fechaUTC.getUTCMonth() + 1).toString().padStart(2, '0')}-${fechaUTC.getUTCDate().toString().padStart(2, '0')}`;
    
                if (!medidasAgrupadasPorDia[dia]) {
                    medidasAgrupadasPorDia[dia] = { Temperatura: 0, Humedad: 0, Presion: 0, Lluvia: 0, count: 0 };
                }
    
                medidasAgrupadasPorDia[dia].Temperatura += item.Temperatura;
                medidasAgrupadasPorDia[dia].Humedad += item.Humedad;
                medidasAgrupadasPorDia[dia].Presion += item.Presion;
                medidasAgrupadasPorDia[dia].Lluvia += item.Lluvia;
    
                medidasAgrupadasPorDia[dia].count++;
            });
    
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const { count, ...medidas } = medidasAgrupadasPorDia[dia];
                const medidasPromedio = {};
    
                Object.keys(medidas).forEach(key => {
                    // Evitar división por cero
                    medidasPromedio[key] = count > 0 ? medidas[key] / count : 0;
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
  
    async getMedidasPromediadasPorMes(req, res) {
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
    
            // Consulta con agregaciones directamente en la base de datos
            const query = {
                query: `
                    SELECT 
                        c["Fecha_local_UTC-5"],
                        AVG(c.Temperatura) AS TemperaturaPromedio,
                        AVG(c.Humedad) AS HumedadPromedio,
                        AVG(c.Presion) AS PresionPromedio,
                        SUM(c.Lluvia) AS TotalLluvia
                    FROM c
                    WHERE c["Fecha_local_UTC-5"] >= @inicio
                      AND c["Fecha_local_UTC-5"] <= @fin
                    GROUP BY c["Fecha_local_UTC-5"]
                    ORDER BY c["Fecha_local_UTC-5"] ASC`,
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

            const esValorValido = (valor, umbral) => {
                return valor !== null && valor !== undefined && !isNaN(valor) && Math.abs(valor) < umbral;
            };
    
            const umbralMaximo = 1e6;
    
            // Agrupar y filtrar datos anómalos
            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;
    
                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 };
                    }
    
                    // Filtrar y agregar solo valores válidos
                    if (esValorValido(item.TemperaturaPromedio, umbralMaximo)) {
                        acc[dia].medidas.Temperatura = (acc[dia].medidas.Temperatura || 0) + item.TemperaturaPromedio;
                    }
    
                    if (esValorValido(item.HumedadPromedio, umbralMaximo)) {
                        acc[dia].medidas.Humedad = (acc[dia].medidas.Humedad || 0) + item.HumedadPromedio;
                    }
    
                    if (esValorValido(item.PresionPromedio, umbralMaximo)) {
                        acc[dia].medidas.Presion = (acc[dia].medidas.Presion || 0) + item.PresionPromedio;
                    }
    
                    if (esValorValido(item.TotalLluvia, umbralMaximo)) {
                        acc[dia].medidas.Lluvia = (acc[dia].medidas.Lluvia || 0) + item.TotalLluvia;
                    }
    
                    acc[dia].count += 1;
                }
                return acc;
            }, {});
    
            // Calcular los promedios para cada día del mes
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const count = medidasAgrupadasPorDia[dia].count;
                const medidasPromedio = {};
    
                // Calcular el promedio de cada medida
                Object.keys(medidasDia).forEach(key => {
                    medidasPromedio[key] = medidasDia[key] / count;
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
    }    

    /******************* CONTROLES PARA TEMPERATURA ***************/

    async getDatosClimaticosPorEscala(req, res) {
        try {
            const { escalaDeTiempo, mes, anio, fechaInicio, fechaFin } = req.body;
    
            // Validaciones iniciales
            if (!escalaDeTiempo && (!mes || !anio) && (!fechaInicio || !fechaFin)) {
                return res.status(400).json({
                    msg: 'Debe proporcionar una escala de tiempo válida (15min, 30min, hora, diaria), un mes/año o un rango de fechas.',
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
    
            let fechaInicioISO;
            let fechaFinISO;
    
            // Obtener la última fecha registrada en la base de datos
            const ultimoRegistroQuery = {
                query: `SELECT TOP 1 c["Fecha_local_UTC-5"]
                        FROM c
                        ORDER BY c["Fecha_local_UTC-5"] DESC`
            };
    
            const { resources: ultimoRegistro } = await container.items.query(ultimoRegistroQuery).fetchAll();
    
            if (ultimoRegistro.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontró ningún registro en la base de datos',
                    code: 404
                });
            }
    
            const ultimaFecha = new Date(ultimoRegistro[0]["Fecha_local_UTC-5"]);
    
            // Establecer rango de fechas según la escala de tiempo o el mes y año
            if (escalaDeTiempo) {
                // Definir fecha de inicio según la escala de tiempo
                switch (escalaDeTiempo) {
                    case '15min':
                        fechaInicioISO = new Date(ultimaFecha.getTime() - 15 * 60000).toISOString();
                        break;
                    case '30min':
                        fechaInicioISO = new Date(ultimaFecha.getTime() - 30 * 60000).toISOString();
                        break;
                    case 'hora':
                        fechaInicioISO = new Date(ultimaFecha.getTime() - 60 * 60000).toISOString();
                        break;
                    case 'diaria':
                        fechaInicioISO = new Date(ultimaFecha.getFullYear(), ultimaFecha.getMonth(), ultimaFecha.getDate()).toISOString();
                        break;
                    default:
                        return res.status(400).json({
                            msg: 'Escala de tiempo inválida. Use 15min, 30min, hora, diaria.',
                            code: 400
                        });
                }
                fechaFinISO = ultimaFecha.toISOString();
            } else if (mes && anio) {
                // Rango de fechas para el mes y año seleccionados
                fechaInicioISO = new Date(anio, mes - 1, 1).toISOString();
                fechaFinISO = new Date(anio, mes, 0).toISOString();
            } else if (fechaInicio && fechaFin) {
                // Rango de fechas específico
                fechaInicioISO = new Date(fechaInicio).toISOString();
                fechaFinISO = new Date(fechaFin).toISOString();
    
                if (isNaN(Date.parse(fechaInicioISO)) || isNaN(Date.parse(fechaFinISO))) {
                    return res.status(400).json({
                        msg: 'Las fechas proporcionadas son inválidas.',
                        code: 400
                    });
                }
            }
    
            // Consulta con operaciones de agregación
            const query = {
                query: `
                    SELECT 
                        AVG(c.Temperatura) AS PromedioTemperatura,
                        MAX(c.Temperatura) AS MaxTemperatura,
                        MIN(c.Temperatura) AS MinTemperatura,
                        AVG(c.Humedad) AS PromedioHumedad,
                        AVG(c.Presion) AS PromedioPresion,
                        SUM(c.Lluvia) AS TotalLluvia
                    FROM c
                    WHERE c["Fecha_local_UTC-5"] >= @inicio
                      AND c["Fecha_local_UTC-5"] <= @fin
                      AND IS_NUMBER(c.Temperatura) AND c.Temperatura >= 1e-5 AND c.Temperatura <= 1e6
                      AND IS_NUMBER(c.Humedad) AND c.Humedad >= 1e-5 AND c.Humedad <= 1e6
                      AND IS_NUMBER(c.Presion) AND c.Presion >= 1e-5 AND c.Presion <= 1e6
                      AND IS_NUMBER(c.Lluvia) AND c.Lluvia >= 0 AND c.Lluvia <= 1e6
                `,
                parameters: [
                    { name: "@inicio", value: fechaInicioISO },
                    { name: "@fin", value: fechaFinISO }
                ]
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0 || !items[0].PromedioTemperatura) {
                return res.status(404).json({
                    msg: `No se encontraron datos climáticos para el rango de fechas proporcionado`,
                    code: 404
                });
            }
    
            const resultado = items[0];
    
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: {
                    promedioTemperatura: resultado.PromedioTemperatura,
                    maxTemperatura: resultado.MaxTemperatura,
                    minTemperatura: resultado.MinTemperatura,
                    promedioHumedad: resultado.PromedioHumedad,
                    promedioPresion: resultado.PromedioPresion,
                    sumaLluvia: resultado.TotalLluvia
                }
            });
    
        } catch (error) {
            console.error('Error en getDatosClimaticosPorEscala:', error);
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala de tiempo',
                code: 500,
                info: error.message
            });
        }
    }
    
    async getAllDatosClimaticosPorEscala(req, res) {
        try {
            const { escalaDeTiempo } = req.body;
    
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
    
            // Calcular los intervalos de tiempo según la escala proporcionada
            const ahora = new Date();
            let fechaInicio;
    
            if (escalaDeTiempo === '15min') {
                fechaInicio = new Date(ahora.getTime() - 15 * 60000 - 300 * 60000); 
            } else if (escalaDeTiempo === '30min') {
                fechaInicio = new Date(ahora.getTime() - 30 * 60000 - 300 * 60000); 
            } else if (escalaDeTiempo === 'hora') {
                fechaInicio = new Date(ahora.getTime() - 60 * 60000 - 300 * 60000);
            } else if (escalaDeTiempo === 'diaria') {
                fechaInicio = new Date(ahora.getFullYear(), ahora.getMonth(), ahora.getDate());
                fechaInicio.setTime(fechaInicio.getTime() - 300 * 60000); 
            } else {
                return res.status(400).json({
                    msg: 'Escala de tiempo inválida. Use 15min, 30min, hora, diaria.',
                    code: 400
                });
            }
    
            const fechaFin = new Date(ahora.getTime() - 300 * 60000); 
            const fechaInicioISO = fechaInicio.toISOString();
            const fechaFinISO = fechaFin.toISOString();
    
            // Consulta para agregar datos en la base de datos
            const query = {
                query: `
                    SELECT 
                        c["Fecha_local_UTC-5"],
                        AVG(c.Temperatura) AS TemperaturaPromedio,
                        AVG(c.Humedad) AS HumedadPromedio,
                        AVG(c.Presion) AS PresionPromedio,
                        SUM(c.Lluvia) AS TotalLluvia
                    FROM c
                    WHERE c["Fecha_local_UTC-5"] >= @inicio
                      AND c["Fecha_local_UTC-5"] <= @fin
                    GROUP BY c["Fecha_local_UTC-5"]
                    ORDER BY c["Fecha_local_UTC-5"] ASC`,
                parameters: [
                    { name: "@inicio", value: fechaInicioISO },
                    { name: "@fin", value: fechaFinISO }
                ]
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (!items || items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos climáticos para el rango de tiempo proporcionado`,
                    code: 404
                });
            }
    
            // Control de valores anómalos
            const esValorValido = (valor, umbral) => {
                return valor !== null && valor !== undefined && !isNaN(valor) && Math.abs(valor) < umbral;
            };
    
            const umbralMaximo = 1e6; // Umbral para valores fuera de rango
    
            // Procesamiento y filtrado de datos
            const datosPorHora = items.map(item => {
                const fecha = item["Fecha_local_UTC-5"];
                const horaExacta = new Date(fecha).toLocaleTimeString('es-ES', { hour12: false, timeZone: 'UTC' });
    
                // Validar cada métrica antes de agregarla
                const temperatura = esValorValido(item.TemperaturaPromedio, umbralMaximo) ? item.TemperaturaPromedio : null;
                const humedad = esValorValido(item.HumedadPromedio, umbralMaximo) ? item.HumedadPromedio : null;
                const presion = esValorValido(item.PresionPromedio, umbralMaximo) ? item.PresionPromedio : null;
                const lluvia = esValorValido(item.TotalLluvia, umbralMaximo) ? item.TotalLluvia : null;
    
                return {
                    hora: horaExacta,
                    medidas: {
                        Temperatura: temperatura,
                        Humedad: humedad,
                        Presion: presion,
                        Lluvia: lluvia
                    }
                };
            }).filter(dato => dato.medidas.Temperatura !== null || dato.medidas.Humedad !== null || dato.medidas.Presion !== null || dato.medidas.Lluvia !== null);
    
            return res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: datosPorHora
            });
    
        } catch (error) {
            console.error('Error en getAllDatosClimaticosPorEscala:', error);
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala de tiempo',
                code: 500,
                info: error.message
            });
        }
    }
    


    async getDatosClimaticosPorEscalaMensual(req, res) {
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
    
            // Adaptar la consulta para hacer los cálculos en la base de datos
            const query = {
                query: `
                    SELECT 
                        c["Fecha_local_UTC-5"],
                        AVG(c.Temperatura) AS TemperaturaPromedio,
                        AVG(c.Humedad) AS HumedadPromedio,
                        AVG(c.Presion) AS PresionPromedio,
                        SUM(c.Lluvia) AS TotalLluvia
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
    
            // Función de validación de valores anómalos
            const esValorValido = (valor, min, max) => {
                return valor !== null && valor !== undefined && valor >= min && valor <= max;
            };
    
            const limites = {
                Temperatura: { min: -50, max: 100 }, 
                Humedad: { min: 0, max: 100 },      
                Presion: { min: 50, max: 2100 },  
                Lluvia: { min: 0, max: 2000 }   
            };
    
            items.forEach(item => {
                const fecha = new Date(item["Fecha_local_UTC-5"]);
                const mes = `${fecha.getFullYear()}-${String(fecha.getMonth() + 1).padStart(2, '0')}`;
    
                if (!datosPorMes[mes]) {
                    datosPorMes[mes] = {
                        TemperaturaTotal: 0,
                        HumedadTotal: 0,
                        PresionTotal: 0,
                        LluviaTotal: 0,
                        totalDatos: 0
                    };
                }
    
                const temperatura = esValorValido(item.TemperaturaPromedio, limites.Temperatura.min, limites.Temperatura.max) ? item.TemperaturaPromedio : 0;
                const humedad = esValorValido(item.HumedadPromedio, limites.Humedad.min, limites.Humedad.max) ? item.HumedadPromedio : 0;
                const presion = esValorValido(item.PresionPromedio, limites.Presion.min, limites.Presion.max) ? item.PresionPromedio : 0;
                const lluvia = esValorValido(item.TotalLluvia, limites.Lluvia.min, limites.Lluvia.max) ? item.TotalLluvia : 0;
    
                if (temperatura || humedad || presion || lluvia) {
                    datosPorMes[mes].TemperaturaTotal += temperatura;
                    datosPorMes[mes].HumedadTotal += humedad;
                    datosPorMes[mes].PresionTotal += presion;
                    datosPorMes[mes].LluviaTotal += lluvia;
                    datosPorMes[mes].totalDatos++;
                }
            });
    
            const resultadosPorMes = Object.keys(datosPorMes).map(mes => {
                const datosMes = datosPorMes[mes];
                return {
                    mes,
                    medidas: {
                        Temperatura: datosMes.TemperaturaTotal / datosMes.totalDatos,
                        Humedad: datosMes.HumedadTotal / datosMes.totalDatos,
                        Presion: datosMes.PresionTotal / datosMes.totalDatos,
                        Lluvia: datosMes.LluviaTotal
                    }
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
    }
    
    async getDatosClimaticosMensual(req, res) {
        try {
            
            
            const { escalaDeTiempo } = req.body;

            console.log("filtro aqui", escalaDeTiempo);
    
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
    
            const query = {
                query: `
                    SELECT 
                        c["Fecha_local_UTC-5"] AS fecha,
                        c.Temperatura,
                        c.Humedad,
                        c.Presion,
                        c.Lluvia
                    FROM c`
            };
    
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron datos climáticos',
                    code: 404
                });
            }
    
            const datosPorMes = {};
    
            // Función de validación de valores anómalos
            const esValorValido = (valor, umbral) => {
                return valor !== null && valor !== undefined && !isNaN(valor) && Math.abs(valor) < umbral;
            };
    
            const umbralMaximo = 1e6; // Umbral para valores fuera de rango
    
            items.forEach(item => {
                const fecha = new Date(item.fecha);
                const mes = `${fecha.getFullYear()}-${String(fecha.getMonth() + 1).padStart(2, '0')}`;
    
                if (!datosPorMes[mes]) {
                    datosPorMes[mes] = {
                        temperaturas: [],
                        humedades: [],
                        presiones: [],
                        lluvias: []
                    };
                }
    
                if (esValorValido(item.Temperatura, umbralMaximo)) {
                    datosPorMes[mes].temperaturas.push(item.Temperatura);
                }
                if (esValorValido(item.Humedad, umbralMaximo)) {
                    datosPorMes[mes].humedades.push(item.Humedad);
                }
                if (esValorValido(item.Presion, umbralMaximo)) {
                    datosPorMes[mes].presiones.push(item.Presion);
                }
                if (esValorValido(item.Lluvia, umbralMaximo)) {
                    datosPorMes[mes].lluvias.push(item.Lluvia);
                }
            });
    
            const resultadosPorMes = Object.keys(datosPorMes).map(mes => {
                const datosMes = datosPorMes[mes];
                const promedioTemperatura = datosMes.temperaturas.length ? datosMes.temperaturas.reduce((acc, val) => acc + val, 0) / datosMes.temperaturas.length : 0;
                const maxTemperatura = datosMes.temperaturas.length ? Math.max(...datosMes.temperaturas) : null;
                const minTemperatura = datosMes.temperaturas.length ? Math.min(...datosMes.temperaturas) : null;
                const promedioHumedad = datosMes.humedades.length ? datosMes.humedades.reduce((acc, val) => acc + val, 0) / datosMes.humedades.length : 0;
                const promedioPresion = datosMes.presiones.length ? datosMes.presiones.reduce((acc, val) => acc + val, 0) / datosMes.presiones.length : 0;
                const sumaLluvia = datosMes.lluvias.length ? datosMes.lluvias.reduce((acc, val) => acc + val, 0) : 0;
    
                return {
                    mes,
                    promedioTemperatura,
                    maxTemperatura,
                    minTemperatura,
                    promedioHumedad,
                    promedioPresion,
                    sumaLluvia
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
    }
    
    
    

}

module.exports = MedidaController;

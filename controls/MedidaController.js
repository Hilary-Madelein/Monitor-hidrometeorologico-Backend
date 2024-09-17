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
    
            const containerId = containers[1];  
            const database = client.database(databaseId);
            const container = database.container(containerId);
    
            // Consulta para obtener el último registro
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
    
            // Responder con el último registro
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,  
                info: items[0] // Devolvemos solo el primer (y único) registro
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
            const { fechaInicio, fechaFin } = req.body; // Leer el rango de fechas del body
    
            // Validar que las fechas existan y sean válidas
            if (!fechaInicio || !fechaFin) {
                return res.status(400).json({
                    msg: 'Debe proporcionar un rango de fechas válido (fechaInicio y fechaFin).',
                    code: 400
                });
            }
    
            const inicio = new Date(fechaInicio);
            const fin = new Date(fechaFin);
    
            // Verificar que las fechas sean válidas
            if (isNaN(inicio.getTime()) || isNaN(fin.getTime())) {
                return res.status(400).json({
                    msg: 'El formato de las fechas es inválido. Deben ser fechas válidas en formato ISO.',
                    code: 400
                });
            }
    
            // Asegurarse de que la fecha de inicio no sea posterior a la de fin
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
    
            // Consulta para obtener las medidas dentro del rango de fechas
            const query = {
                query: `SELECT * 
                        FROM c 
                        WHERE c["Fecha_local_UTC-5"] >= @inicio 
                          AND c["Fecha_local_UTC-5"] <= @fin
                        ORDER BY c["Fecha_local_UTC-5"] DESC`,
                parameters: [
                    { name: "@inicio", value: fechaInicio },
                    { name: "@fin", value: fechaFin }
                ]
            };
    
            // Ejecutar la consulta
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos en el rango de fechas proporcionado`,
                    code: 404
                });
            }
    
            // Definir un rango general razonable para todas las medidas
            const valorMinimo = 1e-5; // Limitar valores cercanos a 0 (pero no exactamente 0)
            const valorMaximo = 1e5;  // Limitar valores extremadamente altos
    
            // Agrupar por día y calcular el promedio de las medidas
            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;  // Agrupar por año-mes-día
    
                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 }; // Inicializa medidas y contador
                    }
    
                    // Iterar sobre cada propiedad (medida) en el item y agregarla a la agrupación si está dentro del rango general
                    Object.keys(item).forEach(key => {
                        if (key !== "Fecha_local_UTC-5" && typeof item[key] === 'number') {
                            const valor = item[key];
                            // Filtrar valores anómalos (demasiado pequeños o demasiado grandes)
                            if (valor >= valorMinimo && valor <= valorMaximo) {
                                if (!acc[dia].medidas[key]) {
                                    acc[dia].medidas[key] = 0;
                                }
                                acc[dia].medidas[key] += valor;  // Sumar la medida si está dentro del rango razonable
                            }
                        }
                    });
                    acc[dia].count += 1; // Contar el número de entradas por día
                }
                return acc;
            }, {});
    
            // Calcular el promedio para cada día
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const count = medidasAgrupadasPorDia[dia].count;
    
                // Calcular el promedio de cada medida
                const medidasPromedio = {};
                Object.keys(medidasDia).forEach(key => {
                    medidasPromedio[key] = medidasDia[key] / count;
                });
    
                return {
                    dia,
                    medidas: medidasPromedio
                };
            });
    
            // Responder con los datos promediados por día
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
            const { mes, anio } = req.body; // Leer el mes y el año del body
    
            // Validar que el mes y el año existan y sean válidos
            if (!mes || !anio) {
                return res.status(400).json({
                    msg: 'Debe proporcionar un mes y año válidos.',
                    code: 400
                });
            }
    
            const mesInt = parseInt(mes);
            const anioInt = parseInt(anio);
    
            // Validar que el mes y el año sean números válidos
            if (isNaN(mesInt) || isNaN(anioInt) || mesInt < 1 || mesInt > 12) {
                return res.status(400).json({
                    msg: 'El mes debe estar entre 1 y 12 y el año debe ser un número válido.',
                    code: 400
                });
            }
    
            // Construir la fecha de inicio y fin del mes
            const fechaInicio = new Date(anioInt, mesInt - 1, 1); // Primer día del mes
            const fechaFin = new Date(anioInt, mesInt, 0); // Último día del mes
    
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
    
            // Consulta para obtener las medidas dentro del rango de fechas (todo el mes)
            const query = {
                query: `SELECT * 
                        FROM c 
                        WHERE c["Fecha_local_UTC-5"] >= @inicio 
                          AND c["Fecha_local_UTC-5"] <= @fin
                        ORDER BY c["Fecha_local_UTC-5"] DESC`,
                parameters: [
                    { name: "@inicio", value: fechaInicio.toISOString() },
                    { name: "@fin", value: fechaFin.toISOString() }
                ]
            };
    
            // Ejecutar la consulta
            const { resources: items } = await container.items.query(query).fetchAll();
    
            if (items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos para el mes proporcionado`,
                    code: 404
                });
            }
    
            // Definir un rango general razonable para las medidas
            const valorMinimo = 1e-5; // Limitar valores cercanos a 0 (pero no exactamente 0)
            const valorMaximo = 1e5;  // Limitar valores extremadamente altos
    
            // Agrupar por día y calcular el promedio de las medidas
            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;  // Agrupar por año-mes-día
    
                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 }; // Inicializa medidas y contador
                    }
    
                    // Iterar sobre cada propiedad (medida) en el item y agregarla a la agrupación si está dentro del rango general
                    Object.keys(item).forEach(key => {
                        if (key !== "Fecha_local_UTC-5" && typeof item[key] === 'number') {
                            const valor = item[key];
                            if (valor >= valorMinimo && valor <= valorMaximo) {
                                if (!acc[dia].medidas[key]) {
                                    acc[dia].medidas[key] = 0;
                                }
                                acc[dia].medidas[key] += valor;  // Sumar la medida
                            }
                        }
                    });
                    acc[dia].count += 1; // Contar el número de entradas por día
                }
                return acc;
            }, {});
    
            // Calcular el promedio para cada día
            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const count = medidasAgrupadasPorDia[dia].count;
    
                // Calcular el promedio de cada medida
                const medidasPromedio = {};
                Object.keys(medidasDia).forEach(key => {
                    medidasPromedio[key] = medidasDia[key] / count;
                });
    
                return {
                    dia,
                    medidas: medidasPromedio
                };
            });
    
            // Responder con los datos promediados por día
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
    
    
    
    
    
        
}

module.exports = MedidaController;

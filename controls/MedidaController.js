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

            const query = {
                query: `SELECT * 
                        FROM c 
                        WHERE c["Fecha_local_UTC-5"] >= @inicio 
                          AND c["Fecha_local_UTC-5"] <= @fin
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

            const valorMinimo = 1e-5;
            const valorMaximo = 1e5;

            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;  // Agrupar por año-mes-día

                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 };
                    }

                    Object.keys(item).forEach(key => {
                        if (key !== "Fecha_local_UTC-5" && typeof item[key] === 'number') {
                            const valor = item[key];
                            if (valor >= valorMinimo && valor <= valorMaximo) {
                                if (!acc[dia].medidas[key]) {
                                    acc[dia].medidas[key] = 0;
                                }
                                acc[dia].medidas[key] += valor;
                            }
                        }
                    });
                    acc[dia].count += 1;
                }
                return acc;
            }, {});

            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const count = medidasAgrupadasPorDia[dia].count;

                const medidasPromedio = {};
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

            const { resources: items } = await container.items.query(query).fetchAll();

            if (items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos para el mes proporcionado`,
                    code: 404
                });
            }

            const valorMinimo = 1e-5;
            const valorMaximo = 1e5;

            const medidasAgrupadasPorDia = items.reduce((acc, item) => {
                const fechaUTC = item["Fecha_local_UTC-5"];
                if (fechaUTC) {
                    const fecha = new Date(fechaUTC);
                    const dia = `${fecha.getUTCFullYear()}-${(fecha.getUTCMonth() + 1).toString().padStart(2, '0')}-${fecha.getUTCDate().toString().padStart(2, '0')}`;

                    if (!acc[dia]) {
                        acc[dia] = { medidas: {}, count: 0 };
                    }

                    Object.keys(item).forEach(key => {
                        if (key !== "Fecha_local_UTC-5" && typeof item[key] === 'number') {
                            const valor = item[key];
                            if (valor >= valorMinimo && valor <= valorMaximo) {
                                if (!acc[dia].medidas[key]) {
                                    acc[dia].medidas[key] = 0;
                                }
                                acc[dia].medidas[key] += valor;
                            }
                        }
                    });
                    acc[dia].count += 1;
                }
                return acc;
            }, {});

            const medidasPromediadasPorDia = Object.keys(medidasAgrupadasPorDia).map(dia => {
                const medidasDia = medidasAgrupadasPorDia[dia].medidas;
                const count = medidasAgrupadasPorDia[dia].count;
                const medidasPromedio = {};

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
                    msg: 'Debe proporcionar una escala de tiempo válida (15min, 30min, hora, diaria) o un mes/año o un rango de fechas.',
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

            // Obtener el último registro
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

            // Cálculo de fechas basado en la escala de tiempo desde el último registro
            if (escalaDeTiempo) {
                let fechaInicio;

                if (escalaDeTiempo === '15min') {
                    fechaInicio = new Date(ultimaFecha.getTime() - 15 * 60000); // Últimos 15 minutos
                } else if (escalaDeTiempo === '30min') {
                    fechaInicio = new Date(ultimaFecha.getTime() - 30 * 60000); // Últimos 30 minutos
                } else if (escalaDeTiempo === 'hora') {
                    fechaInicio = new Date(ultimaFecha.getTime() - 60 * 60000); // Última hora
                } else if (escalaDeTiempo === 'diaria') {
                    fechaInicio = new Date(ultimaFecha.getFullYear(), ultimaFecha.getMonth(), ultimaFecha.getDate()); // Inicio del día
                } else {
                    return res.status(400).json({
                        msg: 'Escala de tiempo inválida. Use 15min, 30min, hora, diaria.',
                        code: 400
                    });
                }

                fechaInicioISO = fechaInicio.toISOString();
                fechaFinISO = ultimaFecha.toISOString();

            } else if (mes && anio) {
                fechaInicioISO = new Date(anio, mes - 1, 1).toISOString(); // Inicio del mes
                fechaFinISO = new Date(anio, mes, 0).toISOString(); // Fin del mes

            } else if (fechaInicio && fechaFin) {
                fechaInicioISO = new Date(fechaInicio).toISOString();
                fechaFinISO = new Date(fechaFin).toISOString();

                if (isNaN(Date.parse(fechaInicioISO)) || isNaN(Date.parse(fechaFinISO))) {
                    return res.status(400).json({
                        msg: 'Las fechas proporcionadas son inválidas.',
                        code: 400
                    });
                }
            }

            const query = {
                query: `SELECT c.Temperatura, c.Humedad, c.Presion, c.Lluvia, c["Fecha_local_UTC-5"]
                        FROM c
                        WHERE c["Fecha_local_UTC-5"] >= @inicio
                          AND c["Fecha_local_UTC-5"] <= @fin
                        ORDER BY c["Fecha_local_UTC-5"] DESC`,
                parameters: [
                    { name: "@inicio", value: fechaInicioISO },
                    { name: "@fin", value: fechaFinISO }
                ]
            };

            const { resources: items } = await container.items.query(query).fetchAll();

            if (items.length === 0) {
                return res.status(404).json({
                    msg: `No se encontraron datos climáticos para el rango de fechas proporcionado`,
                    code: 404
                });
            }

            // Variables para cálculos y umbrales de valores válidos
            const valorMinimo = 1e-5;
            const valorMaximo = 1e6;

            let temperaturaMax = -Infinity;
            let temperaturaMin = Infinity;
            let sumaTemperatura = 0;
            let sumaHumedad = 0;
            let sumaPresion = 0;
            let sumaLluvia = 0;
            let contador = 0;

            // Procesar cada elemento
            items.forEach(item => {
                const temp = item.Temperatura;
                const humedad = item.Humedad;
                const presion = item.Presion;
                const lluvia = item.Lluvia;

                // Procesar temperatura solo si está en el rango válido
                if (temp >= valorMinimo && temp <= valorMaximo) {
                    sumaTemperatura += temp;
                    contador += 1;
                    if (temp > temperaturaMax) temperaturaMax = temp;
                    if (temp < temperaturaMin) temperaturaMin = temp;
                }

                // Procesar humedad si es un número válido
                if (typeof humedad === 'number' && humedad >= valorMinimo && humedad <= valorMaximo) {
                    sumaHumedad += humedad;
                }

                // Procesar presión si es un número válido
                if (typeof presion === 'number' && presion >= valorMinimo && presion <= valorMaximo) {
                    sumaPresion += presion;
                }

                // Procesar lluvia si es un número válido
                if (typeof lluvia === 'number' && lluvia >= 0 && lluvia <= valorMaximo) {
                    sumaLluvia += lluvia;
                }
            });

            // Validar si se encontraron datos válidos
            if (contador === 0) {
                return res.status(404).json({
                    msg: 'No se encontraron datos válidos en los registros',
                    code: 404
                });
            }

            const temperaturaPromedio = sumaTemperatura / contador;
            const humedadPromedio = sumaHumedad / contador;
            const presionPromedio = sumaPresion / contador;

            // Enviar la respuesta con los valores calculados
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: {
                    promedioTemperatura: temperaturaPromedio,
                    maxTemperatura: temperaturaMax,
                    minTemperatura: temperaturaMin,
                    promedioHumedad: humedadPromedio,
                    promedioPresion: presionPromedio,
                    sumaLluvia: sumaLluvia
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

            // Obtener la hora actual (sin ajustar la zona horaria)
            const ahora = new Date();
            let fechaInicio;

            // Cálculo de fechas basado en la escala de tiempo
            if (escalaDeTiempo === '15min') {
                fechaInicio = new Date(ahora.getTime() - 15 * 60000 - 300 * 60000); // Últimos 15 minutos, restando además 300 minutos (5 horas)
            } else if (escalaDeTiempo === '30min') {
                fechaInicio = new Date(ahora.getTime() - 30 * 60000 - 300 * 60000); // Últimos 30 minutos, restando además 300 minutos (5 horas)
            } else if (escalaDeTiempo === 'hora') {
                fechaInicio = new Date(ahora.getTime() - 60 * 60000 - 300 * 60000); // Última hora, restando además 300 minutos (5 horas)
            } else if (escalaDeTiempo === 'diaria') {
                // Si es escala diaria, obtener el inicio del día actual (restando además 5 horas)
                fechaInicio = new Date(ahora.getFullYear(), ahora.getMonth(), ahora.getDate()); // Inicio del día actual
                fechaInicio.setTime(fechaInicio.getTime() - 300 * 60000); // Restar 300 minutos (5 horas) al inicio del día
            } else {
                return res.status(400).json({
                    msg: 'Escala de tiempo inválida. Use 15min, 30min, hora, diaria.',
                    code: 400
                });
            }


            const fechaFin = new Date(ahora.getTime() - 300 * 60000); // Restar 300 minutos (5 horas)
            const fechaInicioISO = fechaInicio.toISOString();
            const fechaFinISO = fechaFin.toISOString();

            const query = {
                query: `SELECT c.Temperatura, c.Humedad, c.Presion, c.Lluvia, c["Fecha_local_UTC-5"]
                        FROM c
                        WHERE c["Fecha_local_UTC-5"] >= @inicio
                          AND c["Fecha_local_UTC-5"] <= @fin
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

            const datosPorHora = items.map(item => {
                // La fecha ya está en UTC-5, no necesitamos crear un objeto Date nuevo
                const fecha = item["Fecha_local_UTC-5"];
                const horaExacta = new Date(fecha).toLocaleTimeString('es-ES', { hour12: false, timeZone: 'UTC' }); // Mostrar directamente la hora sin ajustes
            
                return {
                    hora: horaExacta, // Usar la hora exacta sin aplicar cambios de zona horaria
                    medidas: {
                        Temperatura: item.Temperatura,
                        Humedad: item.Humedad,
                        Presion: item.Presion,
                        Lluvia: item.Lluvia
                    }
                };
            });

            // Enviar la respuesta con los datos
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

            // Consulta para obtener todos los registros climáticos
            const query = {
                query: `SELECT c.Temperatura, c.Humedad, c.Presion, c.Lluvia, c["Fecha_local_UTC-5"]
                        FROM c
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

            // Agrupar los datos por mes
            items.forEach(item => {
                const fecha = new Date(item["Fecha_local_UTC-5"]);
                const mes = `${fecha.getFullYear()}-${String(fecha.getMonth() + 1).padStart(2, '0')}`;

                if (!datosPorMes[mes]) {
                    datosPorMes[mes] = [];
                }

                datosPorMes[mes].push(item);
            });

            // Función para validar si un valor es excesivamente alto
            const esValorValido = (valor, umbral) => {
                return valor && valor < umbral; // El valor debe existir y ser menor que el umbral
            };

            const umbralMaximo = 1e6; // Ajusta este valor según el rango esperado

            // Calcular promedios y sumar los datos por mes
            const resultadosPorMes = Object.keys(datosPorMes).map(mes => {
                const datosMes = datosPorMes[mes];
                let sumaTemperatura = 0;
                let sumaHumedad = 0;
                let sumaPresion = 0;
                let sumaLluvia = 0;
                let totalDatosValidos = 0;

                datosMes.forEach(dato => {
                    const temperaturaValida = esValorValido(dato.Temperatura, umbralMaximo) ? dato.Temperatura : 0;
                    const humedadValida = esValorValido(dato.Humedad, umbralMaximo) ? dato.Humedad : 0;
                    const presionValida = esValorValido(dato.Presion, umbralMaximo) ? dato.Presion : 0;
                    const lluviaValida = esValorValido(dato.Lluvia, umbralMaximo) ? dato.Lluvia : 0;

                    // Sumar solo los valores válidos
                    if (temperaturaValida || humedadValida || presionValida || lluviaValida) {
                        sumaTemperatura += temperaturaValida;
                        sumaHumedad += humedadValida;
                        sumaPresion += presionValida;
                        sumaLluvia += lluviaValida;
                        totalDatosValidos++;
                    }
                });

                // Evitar la división por cero
                if (totalDatosValidos === 0) {
                    totalDatosValidos = 1;
                }

                return {
                    mes: mes,
                    medidas: {
                        Temperatura: sumaTemperatura / totalDatosValidos,
                        Humedad: sumaHumedad / totalDatosValidos,
                        Presion: sumaPresion / totalDatosValidos,
                        Lluvia: sumaLluvia // Total acumulado de lluvia
                    }
                };
            });

            // Enviar los resultados agrupados por mes
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                container: containerId,
                info: resultadosPorMes
            });

        } catch (error) {
            console.error('Error en getDatosClimaticosPorEscalaMensual:', error);
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala mensual',
                code: 500,
                info: error.message
            });
        }
    }



}

module.exports = MedidaController;

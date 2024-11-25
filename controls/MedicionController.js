'use strict';
const models = require('../models');
const { validationResult } = require("express-validator");
const { Op } = require('sequelize');
const Sequelize = require('sequelize');

class MedicionController {

    async getDatosClimaticosPorEscala(req, res) {
        try {
            const { escalaDeTiempo, external_id } = req.body;
    
            // Validaciones de entrada
            if (!external_id) {
                return res.status(400).json({
                    msg: 'Debe proporcionar la estación.',
                    code: 400
                });
            }
    
            if (escalaDeTiempo !== 'mensual') {
                return res.status(400).json({
                    msg: 'Solo se permite la escala de tiempo "mensual".',
                    code: 400
                });
            }
    
            // Obtener las medidas y operaciones válidas para la estación
            const medidasYOperaciones = await models.medida_estacion.findAll({
                where: { estado: 1 },
                include: [
                    {
                        model: models.estacion,
                        where: { external_id },
                        attributes: ['id', 'nombre'],
                        required: true
                    },
                    {
                        model: models.medida_operacion,
                        attributes: ['id_tipo_operacion', 'id_tipo_medida'],
                        include: [
                            {
                                model: models.tipo_medida,
                                attributes: ['id', 'nombre']
                            },
                            {
                                model: models.tipo_operacion,
                                attributes: ['id', 'operacion']
                            }
                        ]
                    }
                ]
            });
    
            // Crear un mapa para asociar medida-estación con las operaciones válidas
            const mapaMedidasOperaciones = {};
            medidasYOperaciones.forEach(me => {
                const idMedidaEstacion = me.id;
                const medidaOperacion = me.medida_operacion;
    
                const medidaNombre = medidaOperacion.tipo_medida.nombre;
                const operacion = medidaOperacion.tipo_operacion.operacion;
    
                if (!mapaMedidasOperaciones[idMedidaEstacion]) {
                    mapaMedidasOperaciones[idMedidaEstacion] = {
                        nombre: medidaNombre,
                        operaciones: []
                    };
                }
                mapaMedidasOperaciones[idMedidaEstacion].operaciones.push(operacion);
            });
    
            // Realizar la consulta para los datos climáticos
            const datosMensuales = await models.medicion.findAll({
                attributes: [
                    [Sequelize.fn('DATE_FORMAT', Sequelize.col('fecha_local'), '%Y-%m'), 'mes'],
                    'id_medida_estacion',
                    [Sequelize.fn('MAX', Sequelize.col('valor')), 'max_valor'],
                    [Sequelize.fn('MIN', Sequelize.col('valor')), 'min_valor'],
                    [Sequelize.fn('SUM', Sequelize.col('valor')), 'suma_valor'],
                    [Sequelize.fn('AVG', Sequelize.col('valor')), 'promedio_valor']
                ],
                include: [
                    {
                        model: models.medida_estacion,
                        attributes: [],
                        required: true,
                        where: {
                            id: Object.keys(mapaMedidasOperaciones)
                        }
                    }
                ],
                group: [
                    [Sequelize.fn('DATE_FORMAT', Sequelize.col('fecha_local'), '%Y-%m')],
                    'id_medida_estacion'
                ],
                order: [[Sequelize.literal('mes'), 'ASC']]
            });
    
            // Procesar los datos para filtrar por operaciones válidas
            const datosPorMes = {};
            datosMensuales.forEach(medicion => {
                const mes = medicion.get('mes');
                const idMedidaEstacion = medicion.get('id_medida_estacion');
                const medida = mapaMedidasOperaciones[idMedidaEstacion];
    
                if (!medida) return; // Ignorar medidas no relacionadas
    
                if (!datosPorMes[mes]) datosPorMes[mes] = { mes, medidas: {} };
    
                if (!datosPorMes[mes].medidas[medida.nombre]) {
                    datosPorMes[mes].medidas[medida.nombre] = {};
                }
    
                // Filtrar métricas por operaciones válidas
                medida.operaciones.forEach(operacion => {
                    if (operacion === 'MAX') {
                        datosPorMes[mes].medidas[medida.nombre][operacion] = parseFloat(medicion.get('max_valor'));
                    }
                    if (operacion === 'MIN') {
                        datosPorMes[mes].medidas[medida.nombre][operacion] = parseFloat(medicion.get('min_valor'));
                    }
                    if (operacion === 'SUMA') {
                        datosPorMes[mes].medidas[medida.nombre][operacion] = parseFloat(medicion.get('suma_valor'));
                    }
                    if (operacion === 'PROMEDIO') {
                        datosPorMes[mes].medidas[medida.nombre][operacion] = parseFloat(medicion.get('promedio_valor'));
                    }
                });
            });
    
            const resultadosPorMes = Object.values(datosPorMes);
    
            // Responder con los resultados
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: resultadosPorMes
            });
        } catch (error) {
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por escala',
                code: 500,
                info: error.message
            });
        }
    }

    async getDatosClimaticosPorRango(req, res) {
        try {
            const { fechaInicio, fechaFin, external_id } = req.body;
    
            // Validaciones de entrada
            if (!external_id) {
                return res.status(400).json({
                    msg: 'Debe proporcionar la estación.',
                    code: 400
                });
            }
    
            if (!fechaInicio || !fechaFin) {
                return res.status(400).json({
                    msg: 'Debe proporcionar ambas fechas: inicio y fin.',
                    code: 400
                });
            }
    
            // Validar que la fecha de inicio sea menor o igual a la fecha de fin
            if (new Date(fechaInicio) > new Date(fechaFin)) {
                return res.status(400).json({
                    msg: 'La fecha de inicio no puede ser mayor que la fecha de fin.',
                    code: 400
                });
            }
    
            // Obtener las medidas y operaciones válidas para la estación
            const medidasYOperaciones = await models.medida_estacion.findAll({
                where: { estado: 1 },
                include: [
                    {
                        model: models.estacion,
                        where: { external_id },
                        attributes: ['id', 'nombre'],
                        required: true
                    },
                    {
                        model: models.medida_operacion,
                        attributes: ['id_tipo_operacion', 'id_tipo_medida'],
                        include: [
                            {
                                model: models.tipo_medida,
                                attributes: ['id', 'nombre']
                            },
                            {
                                model: models.tipo_operacion,
                                attributes: ['id', 'operacion']
                            }
                        ]
                    }
                ]
            });
    
            // Crear un mapa para asociar medida-estación con las operaciones válidas
            const mapaMedidasOperaciones = {};
            medidasYOperaciones.forEach(me => {
                const idMedidaEstacion = me.id;
                const medidaOperacion = me.medida_operacion;
    
                const medidaNombre = medidaOperacion.tipo_medida.nombre;
                const operacion = medidaOperacion.tipo_operacion.operacion;
    
                if (!mapaMedidasOperaciones[idMedidaEstacion]) {
                    mapaMedidasOperaciones[idMedidaEstacion] = {
                        nombre: medidaNombre,
                        operaciones: []
                    };
                }
                mapaMedidasOperaciones[idMedidaEstacion].operaciones.push(operacion);
            });
    
            // Realizar la consulta para los datos climáticos
            const datosPorDias = await models.medicion.findAll({
                attributes: [
                    [Sequelize.fn('DATE', Sequelize.col('fecha_local')), 'dia'],
                    'id_medida_estacion',
                    [Sequelize.fn('MAX', Sequelize.col('valor')), 'max_valor'],
                    [Sequelize.fn('MIN', Sequelize.col('valor')), 'min_valor'],
                    [Sequelize.fn('SUM', Sequelize.col('valor')), 'suma_valor'],
                    [Sequelize.fn('AVG', Sequelize.col('valor')), 'promedio_valor']
                ],
                where: {
                    fecha_local: {
                        [Sequelize.Op.between]: [new Date(fechaInicio), new Date(fechaFin)]
                    }
                },
                include: [
                    {
                        model: models.medida_estacion,
                        attributes: [],
                        required: true,
                        where: {
                            id: Object.keys(mapaMedidasOperaciones)
                        }
                    }
                ],
                group: [
                    [Sequelize.fn('DATE', Sequelize.col('fecha_local'))],
                    'id_medida_estacion'
                ],
                order: [[Sequelize.literal('dia'), 'ASC']]
            });
    
            // Procesar los datos para filtrar por operaciones válidas
            const datosPorDia = {};
            datosPorDias.forEach(medicion => {
                const dia = medicion.get('dia');
                const idMedidaEstacion = medicion.get('id_medida_estacion');
                const medida = mapaMedidasOperaciones[idMedidaEstacion];
    
                if (!medida) return; // Ignorar medidas no relacionadas
    
                if (!datosPorDia[dia]) datosPorDia[dia] = { dia, medidas: {} };
    
                if (!datosPorDia[dia].medidas[medida.nombre]) {
                    datosPorDia[dia].medidas[medida.nombre] = {};
                }
    
                // Filtrar métricas por operaciones válidas
                medida.operaciones.forEach(operacion => {
                    if (operacion === 'MAX') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('max_valor'));
                    }
                    if (operacion === 'MIN') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('min_valor'));
                    }
                    if (operacion === 'SUMA') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('suma_valor'));
                    }
                    if (operacion === 'PROMEDIO') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('promedio_valor'));
                    }
                });
            });
    
            const resultadosPorDia = Object.values(datosPorDia);
    
            // Responder con los resultados
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: resultadosPorDia
            });
        } catch (error) {
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por rango de fechas',
                code: 500,
                info: error.message
            });
        }
    }

    async getDatosClimaticosPorMes(req, res) {
        try {
            const { mes, anio, external_id } = req.body;
    
            // Validaciones de entrada
            if (!external_id) {
                return res.status(400).json({
                    msg: 'Debe proporcionar la estación.',
                    code: 400
                });
            }
    
            if (!mes || !anio) {
                return res.status(400).json({
                    msg: 'Debe proporcionar el mes y el año.',
                    code: 400
                });
            }
    
            // Construir el rango de fechas para el mes y año
            const fechaInicio = new Date(anio, mes - 1, 1); // Primer día del mes
            const fechaFin = new Date(anio, mes, 0); // Último día del mes
    
            // Obtener las medidas y operaciones válidas para la estación
            const medidasYOperaciones = await models.medida_estacion.findAll({
                where: { estado: 1 },
                include: [
                    {
                        model: models.estacion,
                        where: { external_id },
                        attributes: ['id', 'nombre'],
                        required: true
                    },
                    {
                        model: models.medida_operacion,
                        attributes: ['id_tipo_operacion', 'id_tipo_medida'],
                        include: [
                            {
                                model: models.tipo_medida,
                                attributes: ['id', 'nombre']
                            },
                            {
                                model: models.tipo_operacion,
                                attributes: ['id', 'operacion']
                            }
                        ]
                    }
                ]
            });
    
            // Crear un mapa para asociar medida-estación con las operaciones válidas
            const mapaMedidasOperaciones = {};
            medidasYOperaciones.forEach(me => {
                const idMedidaEstacion = me.id;
                const medidaOperacion = me.medida_operacion;
    
                const medidaNombre = medidaOperacion.tipo_medida.nombre;
                const operacion = medidaOperacion.tipo_operacion.operacion;
    
                if (!mapaMedidasOperaciones[idMedidaEstacion]) {
                    mapaMedidasOperaciones[idMedidaEstacion] = {
                        nombre: medidaNombre,
                        operaciones: []
                    };
                }
                mapaMedidasOperaciones[idMedidaEstacion].operaciones.push(operacion);
            });
    
            // Realizar la consulta para los datos climáticos
            const datosPorDias = await models.medicion.findAll({
                attributes: [
                    [Sequelize.fn('DATE', Sequelize.col('fecha_local')), 'dia'],
                    'id_medida_estacion',
                    [Sequelize.fn('MAX', Sequelize.col('valor')), 'max_valor'],
                    [Sequelize.fn('MIN', Sequelize.col('valor')), 'min_valor'],
                    [Sequelize.fn('SUM', Sequelize.col('valor')), 'suma_valor'],
                    [Sequelize.fn('AVG', Sequelize.col('valor')), 'promedio_valor']
                ],
                where: {
                    fecha_local: {
                        [Sequelize.Op.between]: [fechaInicio, fechaFin]
                    }
                },
                include: [
                    {
                        model: models.medida_estacion,
                        attributes: [],
                        required: true,
                        where: {
                            id: Object.keys(mapaMedidasOperaciones)
                        }
                    }
                ],
                group: [
                    [Sequelize.fn('DATE', Sequelize.col('fecha_local'))],
                    'id_medida_estacion'
                ],
                order: [[Sequelize.literal('dia'), 'ASC']]
            });
    
            // Procesar los datos para filtrar por operaciones válidas
            const datosPorDia = {};
            datosPorDias.forEach(medicion => {
                const dia = medicion.get('dia');
                const idMedidaEstacion = medicion.get('id_medida_estacion');
                const medida = mapaMedidasOperaciones[idMedidaEstacion];
    
                if (!medida) return; // Ignorar medidas no relacionadas
    
                if (!datosPorDia[dia]) datosPorDia[dia] = { dia, medidas: {} };
    
                if (!datosPorDia[dia].medidas[medida.nombre]) {
                    datosPorDia[dia].medidas[medida.nombre] = {};
                }
    
                // Filtrar métricas por operaciones válidas
                medida.operaciones.forEach(operacion => {
                    if (operacion === 'MAX') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('max_valor'));
                    }
                    if (operacion === 'MIN') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('min_valor'));
                    }
                    if (operacion === 'SUMA') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('suma_valor'));
                    }
                    if (operacion === 'PROMEDIO') {
                        datosPorDia[dia].medidas[medida.nombre][operacion] = parseFloat(medicion.get('promedio_valor'));
                    }
                });
            });
    
            const resultadosPorDia = Object.values(datosPorDia);
    
            // Responder con los resultados
            res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: resultadosPorDia
            });
        } catch (error) {
            return res.status(500).json({
                msg: 'Se produjo un error al listar los datos climáticos por mes y año',
                code: 500,
                info: error.message
            });
        }
    }
    
    

}

module.exports = MedicionController;

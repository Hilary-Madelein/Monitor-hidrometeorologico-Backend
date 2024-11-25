'use strict';
const { body, validationResult } = require('express-validator');
const models = require('../models');
const uuid = require('uuid');
const path = require('path');
const fs = require('fs');

class MedidaEstacionCOntroller {

    async asignarMedidasEstacion(req, res) {
        const { external_id_estacion, medidas_operaciones } = req.body;
    
        if (!external_id_estacion || !medidas_operaciones || !Array.isArray(medidas_operaciones) || medidas_operaciones.length === 0) {
            return res.status(400).json({ msg: "Datos incompletos: se requieren external_id_estacion y medidas_operaciones como lista", code: 400 });
        }
    
        try {
            const estacion = await models.estacion.findOne({
                where: { external_id: external_id_estacion }
            });
    
            if (!estacion) {
                return res.status(404).json({ msg: "No se encontró la estación con el external_id proporcionado", code: 404 });
            }
    
            const id_estacion = estacion.id;
    
            for (const item of medidas_operaciones) {
                const { external_id_medida_operacion } = item;
    
                if (!external_id_medida_operacion) {
                    return res.status(400).json({ msg: "Cada entrada en medidas_operaciones requiere external_id_medida_operacion", code: 400 });
                }
    
                const medidaOperacion = await models.medida_operacion.findOne({
                    where: { external_id: external_id_medida_operacion }
                });
    
                if (!medidaOperacion) {
                    return res.status(404).json({ 
                        msg: `No se encontró la medida-operación para external_id ${external_id_medida_operacion}`, 
                        code: 404 
                    });
                }
    
                await models.medida_estacion.create({
                    external_id: uuid.v4(),
                    id_estacion: id_estacion,
                    id_medida_operacion: medidaOperacion.id
                });
            }
    
            return res.status(200).json({ msg: "Medidas asignadas correctamente a la estación", code: 200 });
        } catch (error) {
            return res.status(500).json({ msg: error.message || "Ha ocurrido un error en el servidor", code: 500 });
        }
    }
    
    
    
}

module.exports = MedidaEstacionCOntroller;

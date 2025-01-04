'use strict';
const { body, validationResult, check } = require('express-validator');
const models = require('../models');
const path = require('path');
const uuid = require('uuid');
const fs = require('fs');
const microcuenca = require('../models/microcuenca');
const { where } = require('sequelize');

class MicrocuencaController {

    async listarOperativas(req, res) {
        try {
            let listar = await models.microcuenca.findAll({
                where:{estado: true},
                attributes: ['external_id', 'estado', 'foto', 'nombre', 'descripcion'],
            });
            res.json({ msg: 'OK!', code: 200, info: listar });
        } catch (error) {
            res.status(500);
            res.json({ msg: 'Error al listar microcuencas: ' + error.message, code: 500, info: error });
        }
    }
    
    async listar(req, res) {
        try {
            let listar = await models.microcuenca.findAll({
                attributes: ['external_id', 'estado', 'foto', 'nombre', 'descripcion'],
            });
            res.json({ msg: 'OK!', code: 200, info: listar });
        } catch (error) {
            res.status(500);
            res.json({ msg: 'Error al listar microcuencas: ' + error.message, code: 500, info: error });
        }
    }
    

    async obtener(req, res) {
        const external = req.params.external;
        let lista = await models.microcuenca.findOne({
            where: {
                external_id: external
            },
            attributes: [
                'id',
                'nombre',
                'external_id',
                'foto',
                'estado',
                'descripcion'
            ],
        });
        if (lista === null) {
            return res.status(400).json({
                msg: 'NO EXISTE EL REGISTRO',
                code: 400,
                info: lista
            });
        }
        return res.status(200).json({
            msg: 'OK!',
            code: 200,
            info: lista
        });
    }

    async obtenerEstaciones(req, res) {
        try {
            const microcuenca = await models.microcuenca.findAll({
                include: [
                    {
                        model: models.estacion,
                        as: 'estacion', 
                        attributes: [
                            'nombre', 
                            'external_id', 
                            'foto', 
                            'longitud', 
                            'latitud', 
                            'altitud', 
                            'estado', 
                            'tipo', 
                            'id_dispositivo', 
                            'descripcion'
                        ]
                    }
                ]
            });
    
            if (!microcuenca) {
                return res.status(400).json({
                    msg: 'NO EXISTE LA MICROCUENCA',
                    code: 400,
                    info: null
                });
            }

            return res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: microcuenca
            });
            
        } catch (error) {
            console.error(error);
            return res.status(500).json({
                msg: 'ERROR AL OBTENER LOS DATOS',
                code: 500,
                error: error.message
            });
        }
    }

    async obtenerMicrocuencaConEstaciones(req, res) {
        try {
            const microcuenca = await models.microcuenca.findAll({
                attributes: ['nombre', 'descripcion', 'foto', 'estado'],
                include: [
                    {
                        model: models.estacion,
                        as: 'estacion', 
                        attributes: [
                            'nombre', 
                            'external_id', 
                            'foto', 
                            'longitud', 
                            'latitud', 
                            'altitud', 
                            'estado', 
                            'tipo', 
                            'id_dispositivo', 
                            'descripcion'
                        ]
                    }
                ]
            });
    
            if (!microcuenca) {
                return res.status(400).json({
                    msg: 'NO EXISTE LA MICROCUENCA',
                    code: 400,
                    info: null
                });
            }

            return res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: microcuenca
            });
            
        } catch (error) {
            console.error(error);
            return res.status(500).json({
                msg: 'ERROR AL OBTENER LOS DATOS',
                code: 500,
                error: error.message
            });
        }
    }
    
    async guardar(req, res) {
        const transaction = await models.sequelize.transaction();
    
        try {
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                return res.status(400).json({
                    msg: "DATOS INCOMPLETOS",
                    code: 400,
                    errors: errors.array()
                });
            }
        
            const fotoFilename = req.file.filename;
    
            const data = {
                nombre: req.body.nombre,
                foto: fotoFilename,
                descripcion: req.body.descripcion,
                external_id: uuid.v4()
            };
    
            await models.microcuenca.create(data, {transaction});
    
            await transaction.commit();
    
            return res.status(200).json({
                msg: "SE HA REGISTRADO MICROCUENCA CON ÉXITO",
                code: 200
            });
    
        } catch (error) {
            if (req.file?.path) {
                fs.unlinkSync(path.join(__dirname, '../public/images/users', req.file.filename));
            }
    
            if (transaction && !transaction.finished) {
                await transaction.rollback();
            }
    
            return res.status(400).json({
                msg: error.message || "Ha ocurrido un error en el servidor",
                code: 400
            });
        }
    }    

    async modificar(req, res) {
        
        try {
            const microcuencaAux = await models.microcuenca.findOne({
                where: { external_id: req.body.external_id }
            });
    
            if (!microcuencaAux) {
                return res.status(400).json({ msg: "NO EXISTE EL REGISTRO", code: 400 });
            }
    
    
            let imagenAnterior = microcuencaAux.foto;
    
            if (req.file) {
                if (imagenAnterior) {
                    const imagenAnteriorPath = path.join(__dirname, '../public/images/microcuencas/', imagenAnterior);
                    fs.unlink(imagenAnteriorPath, (err) => {
                        if (err) {
                            console.log('Error al eliminar la imagen anterior:', err);
                        } else {
                            console.log("eliminada: " + imagenAnterior);
                        }
                    });
                }
                imagenAnterior = req.file.filename; 
            }
    
            microcuencaAux.nombre = req.body.nombre;
            microcuencaAux.estado = req.body.estado;
            microcuencaAux.foto = imagenAnterior; 
            microcuencaAux.descripcion = req.body.descripcion;
            microcuencaAux.external_id = uuid.v4();
    
            const result = await microcuencaAux.save();
    
            if (!result) {
                return res.status(400).json({ msg: "NO SE HAN MODIFICADO LOS DATOS, VUELVA A INTENTAR", code: 400 });
            }
    
            return res.status(200).json({ msg: "SE HAN MODIFICADO LOS DATOS CON ÉXITO", code: 200 });
        } catch (error) {
            console.error("Error en el servidor:", error);
            return res.status(400).json({ msg: "Error en el servidor", error, code: 400 });
        }
    }
    
    
}
module.exports = MicrocuencaController;
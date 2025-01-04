'use strict';
const { validationResult } = require('express-validator');
const models = require('../models');
const path = require('path');
const uuid = require('uuid');
const fs = require('fs');

class EstacionController {

    async listar(req, res) {
        try {
            let listar = await models.estacion.findAll({
                attributes: ['nombre', 'external_id', 'foto', 'longitud', 'latitud', 'altitud', 'estado', 'tipo', 'id_dispositivo'],
            });
            res.json({ msg: 'OK!', code: 200, info: listar });
        } catch (error) {
            res.status(500);
            res.json({ msg: 'Error al listar estaciones: ' + error.message, code: 500, info: error });
        }
    }

    async listarOperativas(req, res) {
        try {
            let listar = await models.estacion.findAll({
                attributes: ['nombre', 'external_id', 'foto', 'longitud', 'latitud', 'altitud', 'estado', 'tipo', 'id_dispositivo', 'descripcion'],
                where: { estado: 'OPERATIVA' }
            });
            res.json({ msg: 'OK!', code: 200, info: listar });
        } catch (error) {
            console.log(error.message);

            res.status(500);
            res.json({ msg: 'Error al listar estaciones operativas: ' + error.message, code: 500, info: error });
        }
    }

    async obtener(req, res) {
        const external = req.params.external;
        
        let microcuencaAux = await models.microcuenca.findOne({ where: { external_id: external } });
        if (!microcuencaAux) {
            return res.status(400).json({
                msg: "La microcuenca especificada no existe",
                code: 400
            });
        }

        let lista = await models.estacion.findAll({
            where: {
                id_microcuenca: microcuencaAux.id,
            },
            attributes: ['nombre', 'external_id', 'foto', 'longitud', 'latitud', 'altitud', 'estado', 'tipo', 'id_dispositivo', 'descripcion'],
        });
        if (lista === null) {
            return res.status(400).json({
                msg: 'NO EXISTE EL REGISTRO',
                code: 400,
                info: listar
            });
        }
        return res.status(200).json({
            msg: 'OK!',
            code: 200,
            info: lista
        });
    }


    async obtenerPorMicrocuenca(req, res) {
        const external = req.body.external;
        let microcuencaAux = await models.microcuenca.findOne({ where: { external_id: external } });

        let lista = await models.estacion.findAll({
            where: {
                id_microcuenca: microcuencaAux.id,
                estado: "OPERATIVA"
            },
            attributes: ['nombre', 'external_id', 'foto', 'longitud', 'latitud', 'altitud', 'estado', 'tipo', 'id_dispositivo', 'descripcion'],
        });
        if (lista === null) {
            return res.status(400).json({
                msg: 'NO EXISTE EL REGISTRO',
                code: 400,
                info: listar
            });
        }
        return res.status(200).json({
            msg: 'OK!',
            code: 200,
            info: lista
        });
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
            let microcuencaAux = await models.microcuenca.findOne({ where: { external_id: req.body.id_microcuenca } });
            console.log("Valor de microcuencaAux.id:", microcuencaAux ? microcuencaAux.id : "No encontrado");

            if (!microcuencaAux) {
                return res.status(400).json({
                    msg: "La microcuenca especificada no existe",
                    code: 400
                });
            }


            const data = {
                nombre: req.body.nombre,
                longitud: req.body.longitud,
                latitud: req.body.latitud,
                altitud: req.body.altitud,
                estado: req.body.estado,
                tipo: req.body.tipo,
                id_dispositivo: req.body.id_dispositivo,
                foto: fotoFilename,
                descripcion: req.body.descripcion,
                external_id: uuid.v4(),
                id_microcuenca: microcuencaAux.id,
            };

            await models.estacion.create(data, { transaction });
            await transaction.commit();

            return res.status(200).json({
                msg: "SE HAN REGISTRADO LOS DATOS CON ÉXITO",
                code: 200
            });

        } catch (error) {
            if (req.file && req.file.path) {
                fs.unlinkSync(path.join(__dirname, '../public/images/estaciones', req.file.filename));
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
            const estacionAux = await models.estacion.findOne({
                where: { external_id: req.body.external_id }
            });

            if (!estacionAux) {
                return res.status(400).json({ msg: "NO EXISTE EL REGISTRO", code: 400 });
            }

            let imagenAnterior = estacionAux.foto;

            if (req.file) {
                if (imagenAnterior) {
                    const imagenAnteriorPath = path.join(__dirname, '../public/images/estaciones/', imagenAnterior);
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

            estacionAux.nombre = req.body.nombre;
            estacionAux.estado = req.body.estado;
            estacionAux.longitud = req.body.longitud;
            estacionAux.altitud = req.body.altitud;
            estacionAux.latitud = req.body.latitud;
            estacionAux.tipo = req.body.tipo;
            estacionAux.descripcion = req.body.descripcion;
            estacionAux.id_dispositivo = req.body.id_dispositivo;
            estacionAux.foto = imagenAnterior;
            estacionAux.external_id = uuid.v4();

            const result = await estacionAux.save();

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
module.exports = EstacionController;
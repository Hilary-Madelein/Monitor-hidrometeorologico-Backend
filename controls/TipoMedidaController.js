'use strict';
const { body, validationResult } = require('express-validator');
const models = require('../models');
const uuid = require('uuid');
const path = require('path');
const fs = require('fs');

class TipoMedidaController {
    async listar(req, res) {
        try {
            const listar = await models.tipo_medida.findAll({
                where: {
                    estado: true
                },
                attributes: ['nombre', 'icono', 'unidad_medida'],
                include: [
                    {
                        model: models.tipo_operacion,
                        as: 'tipo_operacion',
                        attributes: ['operacion'],
                    },
                ],
            });
            res.json({ msg: 'OK!', code: 200, info: listar });
        } catch (error) {
            res.status(500).json({ msg: 'Error al listar tipos de medida: ' + error.message, code: 500, info: error });
        }
    }

    async obtener(req, res) {
        const external = req.params.external;

        try {
            const tipoMedida = await models.tipo_medida.findOne({
                where: {
                    external_id: external
                },
                attributes: [
                    'id',
                    'nombre',
                    'icono',
                    'unidad_medida'
                ],
                include: [
                    {
                        model: models.tipo_operacion,
                        as: 'tipo_operacion',
                        attributes: ['operacion'],
                    },
                ],
            });

            if (!tipoMedida) {
                return res.status(400).json({
                    msg: 'NO EXISTE EL REGISTRO',
                    code: 400,
                    info: tipoMedida
                });
            }

            return res.status(200).json({
                msg: 'OK!',
                code: 200,
                info: tipoMedida
            });

        } catch (error) {
            res.status(500).json({
                msg: 'Error al obtener el tipo de medida: ' + error.message,
                code: 500,
                info: error
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

            let operaciones = req.body.operaciones;

            if (typeof operaciones === 'string') {
                operaciones = [operaciones];
            }

            if (!Array.isArray(operaciones) || operaciones.length === 0) {
                return res.status(400).json({
                    msg: "Debe especificar al menos una operación",
                    code: 400
                });
            }

            const operacionesValidas = ['PROMEDIO', 'MAX', 'MIN', 'SUMA'];
            const operacionesInvalidas = operaciones.filter(op => !operacionesValidas.includes(op));
            if (operacionesInvalidas.length > 0) {
                return res.status(400).json({
                    msg: "Operaciones inválidas: " + operacionesInvalidas.join(", "),
                    code: 400
                });
            }

            const dataTipoMedida = {
                nombre: req.body.nombre,
                icono: req.file.filename,
                unidad_medida: req.body.unidad_medida,
                externalId: uuid.v4(),
                estado: req.body.estado
            };

            const tipoMedida = await models.tipo_medida.create(dataTipoMedida, { transaction });

            const tipoOperaciones = await models.tipo_operacion.findAll({
                where: {
                    operacion: operaciones
                }
            });

            if (tipoOperaciones.length !== operaciones.length) {
                await transaction.rollback();
                return res.status(400).json({
                    msg: "Una o más operaciones no son válidas",
                    code: 400
                });
            }

            for (const operacion of tipoOperaciones) {
                await models.medida_operacion.create({
                    external_id: uuid.v4(),
                    id_tipo_medida: tipoMedida.id,
                    id_tipo_operacion: operacion.id,
                    estado: true
                }, { transaction });
            }

            await transaction.commit();

            return res.status(200).json({
                msg: "SE HAN REGISTRADO LOS DATOS CON ÉXITO",
                code: 200
            });

        } catch (error) {
            if (transaction && !transaction.finished) {
                await transaction.rollback();
            }

            if (error.name === 'SequelizeUniqueConstraintError' && error.errors[0].path === 'nombre') {
                return res.status(400).json({
                    msg: "ESTE NOMBRE DE TIPO DE MEDIDA YA SE ENCUENTRA REGISTRADO",
                    code: 400
                });
            }

            return res.status(500).json({
                msg: error.message || "Ha ocurrido un error en el servidor",
                code: 500
            });
        }
    }

    async modificar(req, res) {
        try {
            const tipoMedidaAux = await models.tipo_medida.findOne({
                where: { external_id: req.body.external_id }
            });

            if (!tipoMedidaAux) {
                return res.status(400).json({ msg: "NO EXISTE EL REGISTRO", code: 400 });
            }

            let iconoAnterior = tipoMedidaAux.icono;

            if (req.file) {
                if (iconoAnterior) {
                    const iconoAnteriorPath = path.join(__dirname, '../public/images/iconsEstaciones/', iconoAnterior);
                    fs.unlink(iconoAnteriorPath, (err) => {
                        if (err) {
                            console.log('Error al eliminar la imagen anterior:', err);
                        } else {
                            console.log("Icono eliminado: " + iconoAnterior);
                        }
                    });
                }
                iconoAnterior = req.file.filename;
            }

            tipoMedidaAux.nombre = req.body.nombre;
            tipoMedidaAux.icono = iconoAnterior;
            tipoMedidaAux.estado = req.body.estado;
            tipoMedidaAux.unidad_medida = req.body.unidad_medida;
            tipoMedidaAux.external_id = uuid.v4();

            const result = await tipoMedidaAux.save();

            if (!result) {
                return res.status(400).json({ msg: "NO SE HAN MODIFICADO LOS DATOS, VUELVA A INTENTAR", code: 400 });
            }

            if (Array.isArray(req.body.operaciones) && req.body.operaciones.length > 0) {
                await models.tipo_operacion.destroy({
                    where: { id_tipo_medida: tipoMedidaAux.id }
                });

                const operacionesValidas = ['PROMEDIO', 'MAX', 'MIN', 'SUMA'];
                for (const operacion of req.body.operaciones) {
                    if (!operacionesValidas.includes(operacion)) {
                        return res.status(400).json({
                            msg: "Operación inválida: " + operacion,
                            code: 400
                        });
                    }
                    await models.tipo_operacion.create({
                        operacion,
                        id_tipo_medida: tipoMedidaAux.id
                    });
                }
            }

            return res.status(200).json({ msg: "SE HAN MODIFICADO LOS DATOS CON ÉXITO", code: 200 });

        } catch (error) {
            console.error("Error en el servidor:", error);
            return res.status(400).json({ msg: "Error en el servidor", error, code: 400 });
        }
    }
}

module.exports = TipoMedidaController;

'use strict';
module.exports = (sequelize, DataTypes) => {
    const tipo_operacion = sequelize.define('tipo_operacion', {
        operacion: { 
            type: DataTypes.ENUM('PROMEDIO', 'MAX', 'MIN', 'SUMA'), 
            allowNull: false 
        },
        external_id: { 
            type: DataTypes.UUID, 
            defaultValue: DataTypes.UUIDV4,
            unique: true 
        },
        estado: {
            type: DataTypes.BOOLEAN, 
            defaultValue: true 
        },
    }, {
        freezeTableName: true,
        timestamps: false,
    });

    tipo_operacion.associate = function(models) {
        tipo_operacion.hasMany(models.medida_operacion, { 
            foreignKey: 'id_tipo_operacion', 
            as: 'tipo_operacion' 
        });
    };
    tipo_operacion.initializeDefaults = async function() {
        const defaultOperations = ['PROMEDIO', 'MAX', 'MIN', 'SUMA'];
        
        for (const operation of defaultOperations) {
            const exists = await tipo_operacion.findOne({ where: { operacion: operation } });
            
            if (!exists) {
                await tipo_operacion.create({ operacion: operation });
            }
        }
    };

    return tipo_operacion;
};

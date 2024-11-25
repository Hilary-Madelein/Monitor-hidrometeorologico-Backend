'use strict';
module.exports = (sequelize, DataTypes) => {
    const tipo_medida = sequelize.define('tipo_medida', {
        nombre: { type: DataTypes.STRING(20), unique: true, allowNull: false },
        icono: {type: DataTypes.STRING(80), defaultValue: "NO_DATA"},
        unidad_medida: {type: DataTypes.STRING(4), allowNull: false },
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4,unique: true},
        estado:{type: DataTypes.BOOLEAN, defaultValue: true},
    }, {
        freezeTableName: true
    });

    tipo_medida.associate = function(models) {
        tipo_medida.hasMany(models.medida_operacion, { foreignKey: 'id_medida_operacion', as: 'tipo_medida' });
    };

    return tipo_medida;
};

'use strict';
module.exports = (sequelize, DataTypes) => {
    const medida_operacion = sequelize.define('medida_operacion', {
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4, unique: true },
        estado: { type: DataTypes.BOOLEAN, defaultValue: true },
    }, {
        freezeTableName: true
    });

    medida_operacion.associate = function(models) {
        medida_operacion.hasMany(models.medida_estacion, { foreignKey: 'id_medida_operacion', as: 'medida_estacion' });
        medida_operacion.belongsTo(models.tipo_medida, { foreignKey: 'id_tipo_medida' });
        medida_operacion.belongsTo(models.tipo_operacion, { foreignKey: 'id_tipo_operacion' });
    };

    return medida_operacion;
};

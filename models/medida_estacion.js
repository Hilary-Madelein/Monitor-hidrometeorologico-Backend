'use strict';
module.exports = (sequelize, DataTypes) => {
    const medida_estacion = sequelize.define('medida_estacion', {
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4, unique: true },
        estado: { type: DataTypes.BOOLEAN, defaultValue: true },
    }, {
        freezeTableName: true
    });

    medida_estacion.associate = function(models) {
        medida_estacion.belongsTo(models.medida_operacion, { foreignKey: 'id_medida_operacion' });
        medida_estacion.belongsTo(models.estacion, { foreignKey: 'id_estacion' });
        medida_estacion.hasMany(models.medicion, { foreignKey: 'id_medida_estacion', as: 'medicion' });
    };

    return medida_estacion;
};

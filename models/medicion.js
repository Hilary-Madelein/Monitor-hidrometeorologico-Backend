'use strict';
module.exports = (sequelize, DataTypes) => {
    const medicion = sequelize.define('medicion', {
        fecha_local: { type: DataTypes.DATE, allowNull: false },
        valor: { type: DataTypes.DECIMAL(10, 2), allowNull: false },
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4, unique: true },
        estado: { type: DataTypes.BOOLEAN, defaultValue: true },
    }, {
        freezeTableName: true,
        timestamps: false,
    });
    
    medicion.associate = function(models) {
        medicion.belongsTo(models.medida_estacion, { foreignKey: 'id_medida_estacion' });
    };

    return medicion;
};

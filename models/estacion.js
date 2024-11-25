'use strict';
module.exports = (sequelize, DataTypes) => {
    const estacion = sequelize.define('estacion', {
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4, unique: true},
        estado:{type: DataTypes.ENUM('OPERATIVA', 'MANTENIMIENTO', 'NO_OPERATIVA'), allowNull: false, defaultValue: 'OPERATIVA'},
        foto: { type: DataTypes.STRING(80), defaultValue: "NO_DATA"},
        nombre: { type: DataTypes.STRING(20), defaultValue: "NO_DATA" },
        longitud: { type: DataTypes.DOUBLE(10, 8), allowNull: false },
        latitud: { type: DataTypes.DOUBLE(10, 8), allowNull: false},
        altitud: { type: DataTypes.DOUBLE(6, 2), allowNull: false},
        id_dispositivo: { type: DataTypes.STRING(20), defaultValue: "NO_DATA", unique: true, allowNull: false},
        tipo: {type: DataTypes.ENUM('METEOROLOGICA', 'HIDROLOGICA', 'PLUVIOMETRICA'), allowNull: false, defaultValue: 'METEOROLOGICA'},
        descripcion: { type: DataTypes.STRING(150), defaultValue: "NO_DATA" }
    }, {
        freezeTableName: true
    });
    estacion.associate = function (models){
        estacion.belongsTo(models.estacion, {foreignKey: 'id_microcuenca'});
        estacion.hasMany(models.medida_estacion, { foreignKey: 'id_estacion', as: 'medida_estacion' });
    };
 
    return estacion;
};
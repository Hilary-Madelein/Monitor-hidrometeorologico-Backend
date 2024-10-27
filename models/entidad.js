'use strict';
module.exports = (sequelize, DataTypes) => {
    const entidad = sequelize.define('entidad', {
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4,unique: true},
        estado:{type: DataTypes.BOOLEAN, defaultValue: true},
        foto: { type: DataTypes.STRING(80), defaultValue: "NO_DATA"},
        nombres: { type: DataTypes.STRING(20), defaultValue: "NO_DATA" },
        apellidos: { type: DataTypes.STRING(20), defaultValue: "NO_DATA" },
        telefono:  { type: DataTypes.STRING(20), defaultValue: "NO_DATA"},
        rol: {type: DataTypes.ENUM('ADMINISTRADOR', 'USUARIO_GENERAL'), allowNull: false, defaultValue: 'ADMINISTRADOR'},
    }, {
        freezeTableName: true
    });
    entidad.associate = function (models){
        entidad.hasOne(models.cuenta, { foreignKey: 'id_entidad', as: 'cuenta'});
    };
 
    return entidad;
};
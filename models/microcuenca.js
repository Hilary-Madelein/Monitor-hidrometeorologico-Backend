'use strict';
module.exports = (sequelize, DataTypes) => {
    const microcuenca = sequelize.define('microcuenca', {
        external_id: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4,unique: true},
        estado:{type: DataTypes.BOOLEAN, defaultValue: true},
        foto: { type: DataTypes.STRING(80), defaultValue: "NO_DATA"},
        nombre: { type: DataTypes.STRING(30), defaultValue: "NO_DATA" },
        descripcion: { type: DataTypes.STRING(150), defaultValue: "NO_DATA" }
    }, {
        freezeTableName: true
    });
    microcuenca.associate = function (models){
        microcuenca.hasMany(models.estacion, {foreignKey: 'id_microcuenca',as:'estacion'});
    };
 
    return microcuenca;
};
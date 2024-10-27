var express = require('express');
var router = express.Router();
const AbortController = require('abort-controller');
require('dotenv').config();  // Cargar las variables de entorno

const { CosmosClient } = require('@azure/cosmos');

// Definir AbortController globalmente si no existe
if (typeof global.AbortController === 'undefined') {
  global.AbortController = AbortController;
}

// Cargar variables de entorno para Cosmos DB
const endpoint = process.env.COSMOS_ENDPOINT;
const key = process.env.COSMOS_KEY;
const databaseId = "PUEAR";

// Crear cliente de Cosmos
const client = new CosmosClient({ endpoint, key });

router.get('/privado/:external', async function(req, res, next) {
  const llave = req.params.external;
  const envKey = process.env.KEY_SQ;  

  console.log("Llave recibida:", llave);  // Imprimir la llave recibida
  console.log("Llave esperada (desde .env):", envKey);  // Imprimir la llave esperada desde .env

  if (llave === envKey) {
    try {
      // Conexión a Cosmos DB
      const database = client.database(databaseId);
      console.log(`Conectado a la base de datos Cosmos: ${databaseId}`);
      
      // Conexión a MySQL
      var models = require('./../models');  // Cargar el modelo para MySQL
      await models.sequelize.sync();  // Conectar y sincronizar MySQL
      console.log('Se ha conectado a MySQL');

      res.status(200).send(`Conectado a la base de datos Cosmos: ${databaseId} y MySQL`);
    } catch (err) {
      console.error('Error conectando a Cosmos DB o MySQL:', err);
      res.status(500).json({
        message: 'Error conectando a las bases de datos',
        error: err.message
      });
    }
  } else {
    res.status(401).json({ message: 'Llave incorrecta!' });
  }
});


async function getAllContainers() {
  try {
    const database = client.database(databaseId);
    
    const { resources: containers } = await database.containers.readAll().fetchAll();
    
    return containers.map(container => container.id);
  } catch (err) {
    console.error('Error obteniendo contenedores:', err);
    throw new Error('No se pudo obtener la lista de contenedores');
  }
}

module.exports = {
  router,
  client,
  databaseId,
  getAllContainers  
};

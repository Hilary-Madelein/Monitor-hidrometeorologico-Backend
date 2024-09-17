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

// Ruta para verificar la clave y conectarse a Cosmos DB
router.get('/privado/:external', async function(req, res, next) {
  const llave = req.params.external;
  const envKey = process.env.KEY_SQ;  

  if (llave === envKey) {
    try {
      // Establecer conexión con la base de datos
      const database = client.database(databaseId);
      console.log(`Conectado a la base de datos: ${databaseId}`);
      res.status(200).send(`Conectado a la base de datos: ${databaseId}`);
    } catch (err) {
      console.error('Error conectando a Cosmos DB:', err);
      res.status(500).json({
        message: 'Error conectando a la base de datos',
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

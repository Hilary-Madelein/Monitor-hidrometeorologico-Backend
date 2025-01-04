var express = require('express');
var router = express.Router();
let jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const uuid = require('uuid');
const { body, validationResult,isDate } = require('express-validator');
const MedidaController = require('../controls/MedidaController');
let medidaController = new MedidaController();
const EntidadController = require('../controls/EntidadController');
let entidadController = new EntidadController();
const CuentaController = require('../controls/CuentaController');
let cuentaController = new CuentaController();
const MicrocuencaController = require('../controls/MicrocuencaController');
let microcuencaController = new MicrocuencaController();
const EstacionController = require('../controls/EstacionController');
let estacionController = new EstacionController();
const TipoMedidaController = require('../controls/TipoMedidaController');
let tipoMedidaController = new TipoMedidaController();
const MedicionController = require('../controls/MedicionController');
let medicionController = new MedicionController();
const MedidaEstacionController = require('../controls/MedidaEstacionController');
let medidaEstacionController = new MedidaEstacionController();

/* GET users listing. */
router.get('/', function (req, res, next) {
  res.json({ "version": "1.0", "name": "hidrometeorologica-backend" });
});

let auth = function middleware(req, res, next) {
  const token = req.headers['x-api-token'];
  if (token) {
    require('dotenv').config();
    const llave = process.env.KEY;
    jwt.verify(token, llave, async (err, decoded) => {
      if (err) {
        res.status(401);
        res.json({
          msg: "Token no valido",
          code: 401
        });
      } else {
        const models = require('../models');
        const cuenta = models.cuenta;
        req.decoded = decoded;
        let aux = await cuenta.findOne({ 
          where: { 
            external_id: req.decoded.external 
          } 
        })
        if (aux === null) {
          res.status(401);
          res.json({
            msg: "Token no valido o expirado",
            code: 401
          });
        } else {
          next();
        }
      }
    });
  } else {
    res.status(401);
    res.json({
      msg: "No existe token",
      code: 401
    });
  }

};


// GUARDAR IMAGENES 

// Función para crear configuraciones de almacenamiento de multer
const createStorage = (folderPath) => {
  return multer.diskStorage({
    destination: path.join(__dirname, folderPath),
    filename: (req, file, cb) => {
      const parts = file.originalname.split('.');
      const extension = parts[parts.length - 1];
      cb(null, uuid.v4() + "." + extension);
    }
  });
};

// Método para validar las extensiones de las fotografías
const extensionesAceptadasFoto = (req, file, cb) => {
  const allowedExtensions = ['.jpeg', '.jpg', '.png'];
  console.log(file);
  const ext = path.extname(file.originalname).toLowerCase();
  if (allowedExtensions.includes(ext)) {
    cb(null, true);
  } else {
    cb(new Error('Solo se permiten archivos JPEG, JPG y PNG.'), false);
  }
};

// Configuración de Multer con control de tamaño y tipo de archivo
const uploadFoto = (folderPath) => {
  const storage = createStorage(folderPath);
  return multer({
    storage: storage,
    fileFilter: extensionesAceptadasFoto,
    limits: {
      fileSize: 2 * 1024 * 1024  // 5MB
    }
  });
};


const uploadFotoPersona = uploadFoto('../public/images/users');
const uploadFotoMicrocuenca = uploadFoto('../public/images/microcuencas');
const uploadFotoEstacion = uploadFoto('../public/images/estaciones');
const uploadIconoEstacion = uploadFoto('../public/images/icons_estaciones');

// Ruta para obtener los últimos 10 registros de los contenedores EMA y EHA
router.get('/listar/ultimasMedidasTen', medidaController.getUltimasTenMedidas);


/** RUTAS DE MEDIDA */
router.post('/listar/medidas/diaria', medidaController.getMedidasPromediadasPorDia);
//router.post('/listar/medidas/mes', medidaController.getMedidasPromediadasPorMes);
router.get('/listar/ultimaMedida', medidaController.getUltimaMedicion);
router.post('/listar/medidas/escala', medidaController.getDatosClimaticosPorEscala);
router.post('/listar/todasMedidas/escala', medidaController.getAllDatosClimaticosPorEscala);
//router.post('/listar/temperatura/mensual', medidaController.getDatosClimaticosPorEscalaMensual);
//router.post('/listar/temperatura/mensual/datos', medidaController.getDatosClimaticosMensual);

/**
 * RUTAS DE PERSONA
 */

router.post('/guardar/entidad', (req, res, next) => {
  uploadFotoPersona.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    entidadController.guardar(req, res, next);
  });
});

router.put('/modificar/entidad', (req, res, next) => {
  uploadFotoPersona.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    entidadController.modificar(req, res, next);
  });
});
router.get('/listar/entidad', entidadController.listar);
router.get('/obtener/entidad/:external',  entidadController.obtener);

/**
 * RUTAS DE CUENTA
 */

router.post('/sesion', [
  body('correo', 'Ingrese un correo valido').exists().not().isEmpty().isEmail(),
  body('clave', 'Ingrese una clave valido').exists().not().isEmpty(),
], cuentaController.sesion)


/**
 * RUTAS DE MICROCUENCAS
 */

router.post('/guardar/microcuenca', (req, res, next) => {
  uploadFotoMicrocuenca.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    microcuencaController.guardar(req, res, next);
  });
});

router.put('/modificar/microcuenca', (req, res, next) => {
  uploadFotoMicrocuenca.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    microcuencaController.modificar(req, res, next);
  });
});
router.get('/listar/microcuenca', microcuencaController.listar);
router.get('/listar/microcuenca/operativas', microcuencaController.listarOperativas);
router.get('/obtener/microcuenca/:external',  microcuencaController.obtener);
router.get('/microcuenca/estaciones', microcuencaController.obtenerMicrocuencaConEstaciones);


/**
 * RUTAS DE ESTACIONES
 */

router.post('/guardar/estacion', (req, res, next) => {
  uploadFotoEstacion.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    estacionController.guardar(req, res, next);
  });
});

router.put('/modificar/estacion', (req, res, next) => {
  uploadFotoEstacion.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    estacionController.modificar(req, res, next);
  });
});
router.get('/listar/estacion', estacionController.listar);
router.get('/listar/estacion/operativas', estacionController.listarOperativas);
router.get('/obtener/estacion/:external',  estacionController.obtener);
router.post('/estaciones/operativas/microcuenca', estacionController.obtenerPorMicrocuenca)

/**
 * RUTAS DE TIPOS DE MEDIDAS
 */

router.post('/guardar/tipo_medida', (req, res, next) => {
  uploadIconoEstacion.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error,
        code: 400
      });
    }
    tipoMedidaController.guardar(req, res, next);
  });
});

router.put('/modificar/tipo_medida', (req, res, next) => {
  uploadIconoEstacion.single('foto')(req, res, (error) => {
    if (error) {
      if (error instanceof multer.MulterError && error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({
          msg: "El archivo es demasiado grande. Por favor, sube un archivo de menos de 5 MB.",
          code: 413
        });
      }
      return res.status(400).json({
        msg: "Error al cargar el archivo: " + error.message,
        code: 400
      });
    }
    tipoMedidaController.modificar(req, res, next);
  });
});
router.get('/listar/tipo_medida', tipoMedidaController.listar);
router.get('/obtener/tipo_medida/:external', tipoMedidaController.obtener);

/**
 * RUTAS DE MEDIDA ESTACION
 */

router.post('/asignar/medidas/estaciones', medidaEstacionController.asignarMedidasEstacion);

/**
 * RUTAS DE MEDICION CONTROLLER
 */

router.post('/medidas/mensuales/promediadas', medicionController.getDatosClimaticosPorEscala);
router.post('/medidas/rango/promediadas', medicionController.getDatosClimaticosPorRango);
router.post('/medidas/desglosemes/promediadas', medicionController.getDatosClimaticosPorMes);

module.exports = router;

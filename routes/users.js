var express = require('express');
var router = express.Router();
const MedidaController = require('../controls/MedidaController');
var medidaController = new MedidaController();

/* GET users listing. */
router.get('/', function (req, res, next) {
  res.json({ "version": "1.0", "name": "hidrometeorologica-backend" });
});

// Ruta para obtener los últimos 10 registros de los contenedores EMA y EHA
router.get('/listar/ultimasMedidasTen', medidaController.getUltimasTenMedidas);

router.get('/listar/medidas/diaria', medidaController.getMedidasPromediadasPorDia);
router.get('/listar/medidas/mes', medidaController.getMedidasPromediadasPorMes);
router.get('/listar/ultimaMedida', medidaController.getUltimaMedicion);


module.exports = router;

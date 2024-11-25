const MigracionController = require('../controls/MigracionController');
var migracionController = new MigracionController();

// Ejecución inicial de migración al iniciar el servidor
(async () => {
  console.log('Ejecución inicial de migración al iniciar el servidor...');
  try {
    const { inicio, fin } = await migracionController.obtenerFechasParaMigrar();

    if (inicio < fin) {
      const fechasParaMigrar = migracionController.generarRangoFechas(inicio, fin);

      for (const fecha of fechasParaMigrar) {
        await migracionController.migrarYCalcularMedicionesParaUnDiaEspecifico(fecha); // Migrar solo ese día
        console.log(`Migración realizada para el día: ${fecha}`);
      }
    } else {
      console.log('No hay días completos para migrar.');
    }
  } catch (error) {
    console.error('Error en la migración inicial:', error);
  }
})();

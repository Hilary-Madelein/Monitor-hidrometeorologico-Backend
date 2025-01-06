var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var indexRouter = require('./routes/index').router;  // Importar solo el router
var usersRouter = require('./routes/users');
const cors = require('cors');
var createError = require('http-errors');


var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use(cors({ origin: '*' }));


app.use('/', indexRouter);  // Usar el enrutador aquí
app.use('/api', usersRouter);

app.get('/', (req, res) => {
  res.status(200).send('Hello, World!');
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

app.use(function(req, res, next) {
  next(createError(404));  // Lanza un error 404 si no se encuentra la ruta
});


app.use(function(err, req, res, next) {
  // Establece los valores por defecto para los errores
  res.status(err.status || 500);
  res.json({
    message: err.message,
    error: err
  });
});



module.exports = app;

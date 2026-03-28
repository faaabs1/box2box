require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const express = require('express');
const path = require('path');

const apiRouter = require('./routes/api');
const pagesRouter = require('./routes/pages');

const app = express();
const PORT = process.env.PORT || 3000;

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

app.use('/api', apiRouter);
app.use('/', pagesRouter);

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).render('error', { message: err.message });
});

app.listen(PORT, () => {
  console.log(`Box2Box Dashboard running at http://localhost:${PORT}`);
});

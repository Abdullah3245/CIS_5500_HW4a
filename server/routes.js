const { Pool, types } = require('pg');
const config = require('./config.json');
const { off } = require('process');

// Override the default parsing for BIGINT (PostgreSQL type ID 20)
types.setTypeParser(20, val => parseInt(val, 10)); //DO NOT DELETE THIS

// Create PostgreSQL connection using database credentials provided in config.json
// Do not edit. If the connection fails, make sure to check that config.json is filled out correctly
const connection = new Pool({
  host: config.rds_host,
  user: config.rds_user,
  password: config.rds_password,
  port: config.rds_port,
  database: config.rds_db,
  ssl: {
    rejectUnauthorized: false,
  },
});
connection.connect((err) => err && console.log(err));

/******************
 * WARM UP ROUTES *
 ******************/

// Route 1: GET /author/:type
const author = async function (req, res) {
  // TODO (TASK 1): replace the values of name and pennkey with your own
  const name = 'Muhammad Abdullah Goher';
  const pennkey = 'mgoher';
  // checks the value of type in the request parameters
  // note that parameters are required and are specified in server.js in the endpoint by a colon (e.g. /author/:type)
  if (req.params.type === 'name') {
    // res.json returns data back to the requester via an HTTP response
    res.json({ data: name });
  } else if (req.params.type === 'pennkey') {
    // TODO (TASK 2): edit the else if condition to check if the request parameter is 'pennkey' and if so, send back a JSON response with the pennkey
    res.json({ data: pennkey });
  } else {
    res.status(400).json({});
  }
}

// Route 2: GET /random
const random = async function (req, res) {
  // you can use a ternary operator to check the value of request query values
  // which can be particularly useful for setting the default value of queries
  // note if users do not provide a value for the query it will be undefined, which is falsey
  const explicit = req.query.explicit === 'true' ? 1 : 0;

  // Here is a complete example of how to query the database in JavaScript.
  // Only a small change (unrelated to querying) is required for TASK 3 in this route.
  connection.query(`
    SELECT *
    FROM Songs
    WHERE explicit <= ${explicit}
    ORDER BY RANDOM()
    LIMIT 1
  `, (err, data) => {
    if (err) {
      // If there is an error for some reason, print the error message and
      // return an empty object instead
      console.log(err);
      // Be cognizant of the fact we return an empty object {}. For future routes, depending on the
      // return type you may need to return an empty array [] instead.
      res.json({});
    } else {
      res.json({
        song_id: data.rows[0].song_id,
        title: data.rows[0].title
      });
    }
  });
}

/********************************
 * BASIC SONG/ALBUM INFO ROUTES *
 ********************************/

// Route 3: GET /song/:song_id
const song = async function (req, res) {
  const id = req.params.song_id;
  console.log("here is the id:", id);
  connection.query(`
    SELECT *
    FROM Songs
    WHERE song_id = '${id}'
    `, (err, data) => {
    if (err) {
      console.log(err);
      res.json({});
    } else {
      res.json(data.rows[0] || {});
    }
  });
}

// Route 4: GET /album/:album_id
const album = async function (req, res) {
  const album_id = req.params.album_id;
  connection.query(`
    SELECT *
    FROM albums
    WHERE album_id = '${album_id}'
    `
    , (err, data) => {
      if (err) {
        console.log(err);
        res.json({});
      } else {
        res.json(data.rows[0]);
      }
    }
  );
}

// Route 5: GET /albums
const albums = async function (req, res) {
  connection.query(`
    SELECT *
    FROM albums
    ORDER BY release_date DESC
    `, (err, data) => {
    if (err) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data.rows);
    }
  });
}

// Route 6: GET /album_songs/:album_id
const album_songs = async function (req, res) {
  const album_id = req.params.album_id;
  connection.query(`
    SELECT song_id, title, number, duration, plays
    FROM songs
    WHERE album_id = '${album_id}'
    ORDER BY number ASC
    `, (err, data) => {
    if (err) {
      console.log(err);
      res.json({});
    } else {
      res.json(data.rows);
    }
  }
  );
}

/************************
 * ADVANCED INFO ROUTES *
 ************************/

// Route 7: GET /top_songs
const top_songs = async function (req, res) {
  const page = req.query.page;
  const pageSize = req.query.page_size ?? 10;

  if (!page) {
    connection.query(`
      SELECT s.song_id, s.title, s.album_id, a.title AS album, s.plays
      FROM songs s
      JOIN albums a on s.album_id = a.album_id
      ORDER BY plays DESC
      `, (err, data) => {
      if (err) {
        console.log(err);
        res.json({});
      } else {
        res.json(data.rows);
      }
    }
    );
  } else {
    const offset = (page - 1) * pageSize;
    connection.query(`
      SELECT s.song_id, s.title, s.album_id, a.title AS album, s.plays
      FROM Songs s
      JOIN albums a on s.album_id = a.album_id
      ORDER BY plays DESC
      LIMIT ${pageSize} OFFSET ${offset}
      `, (err, data) => {
      if (err) {
        console.log(err);
        res.json({})
      } else {
        res.json(data.rows);
      }
    });
  }
}

// Route 8: GET /top_albums
const top_albums = async function (req, res) {
  const page = req.query.page;
  const pageSize = req.query.page_size ?? 10;
  if (!page) {
    connection.query(`
        SELECT a.album_id, a.title, SUM(s.plays) AS plays
        FROM albums a
        JOIN songs s on s.album_id = a.album_id
        GROUP BY a.album_id, a.title
        ORDER BY plays DESC
        `, (err, data) => {
      if (err) {
        console.log(err);
        res.json({});
      } else {
        res.json(data.rows);
      }
    })
  } else {
    const offset = (page - 1) * pageSize;
    connection.query(`
        SELECT a.album_id, a.title, SUM(s.plays) AS plays
        FROM albums a
        JOIN songs s on s.album_id = a.album_id
        GROUP BY a.album_id, a.title
        ORDER BY plays DESC
        LIMIT ${pageSize} OFFSET ${offset}
        `, (err, data) => {
      if (err) {
        console.log(err);
        res.json({});
      } else {
        res.json(data.rows);
      }
    })
  }
}

// Route 9: GET /search_songs
const search_songs = async function (req, res) {
  // defining search parameter
  const title = req.query.title ?? '';
  const durationLow = req.query.duration_low ?? 60;
  const durationHigh = req.query.duration_high ?? 660;
  const playsLow = req.query.plays_low ?? 0;
  const playsHigh = req.query.plays_high ?? 1100000000;
  const danceabilityLow = req.query.danceability_low ?? 0;
  const danceabilityHigh = req.query.danceability_high ?? 1;
  const energyLow = req.query.energy_low ?? 0;
  const energyHigh = req.query.energy_high ?? 1;
  const valenceLow = req.query.valence_low ?? 0;
  const valenceHigh = req.query.valence_high ?? 1;
  const explicit = req.query.explicit === 'true' ? 1 : 0;

  // querying the database
  connection.query(`
    SELECT song_id, album_id, title, number, duration, plays,
           danceability, energy, valence, tempo, key_mode, explicit
    FROM Songs
    WHERE title LIKE '%${title}%'
      AND duration BETWEEN ${durationLow} AND ${durationHigh}
      AND plays BETWEEN ${playsLow} AND ${playsHigh}
      AND danceability BETWEEN ${danceabilityLow} AND ${danceabilityHigh}
      AND energy BETWEEN ${energyLow} AND ${energyHigh}
      AND valence BETWEEN ${valenceLow} AND ${valenceHigh}
      AND explicit <= ${explicit}
    ORDER BY title ASC;
    `, (err, data) => {
    if (err) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data.rows || []);;
    }
  }
    ,)
}

/**
 * Route 10: GET /playlist/entrance_songs - Wedding entrance playlist
 *
 * Let's celebrate the wedding of Travis and Taylor!
 *
 * Travis Kelce is cooking up some slow danceable songs with Taylors before the
 * highly anticipated Wedding entrance. Travis decides that a slow danceable
 * song is one with: maximum energy of 0.5 and a minimum danceability of at least 0.73
 * Let's design a wedding entrance playlist for Travis to pass to the DJ
 */
const entrance_songs = async function (req, res) {
  // Allow the user to specify how many songs they want (limit) with a default of 10
  const limit = req.query.limit || 10;
  const maxEnergy = req.query.max_energy || 0.5;
  const minDanceability = req.query.min_danceability || 0.73;
  connection.query(`
    SELECT s.song_id, s.title, a.title AS album, s.danceability, s.energy, s.valence
    FROM Songs s
    JOIN Albums a ON s.album_id = a.album_id
    WHERE s.energy <= ${maxEnergy}
      AND s.danceability >= ${minDanceability}
    ORDER BY s.valence DESC, s.danceability DESC
    LIMIT ${limit};
    `, (err, data) => {
    if (err) {
      console.log(err);
      res.json([]);
    } else {
      res.json(data.rows || []);
    }
  });
}

module.exports = {
  author,
  random,
  song,
  album,
  albums,
  album_songs,
  top_songs,
  top_albums,
  search_songs,
  entrance_songs
}

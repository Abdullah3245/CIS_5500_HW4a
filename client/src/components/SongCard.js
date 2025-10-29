import { useEffect, useState } from 'react';
import { Box, Button, ButtonGroup, Modal } from '@mui/material';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis } from 'recharts';
import { NavLink } from 'react-router-dom';

import { formatDuration } from '../helpers/formatter';
const config = require('../config.json');

// SongCard is a modal (a common example of a modal is a dialog window).
// Typically, modals will conditionally appear (specified by the Modal's open property)
// but in our implementation whether the Modal is open is handled by the parent component
// (see HomePage.js for example), since it depends on the state (selectedSongId) of the parent
export default function SongCard({ songId, handleClose }) {
  const [songData, setSongData] = useState({});
  const [albumData, setAlbumData] = useState({});

  const [barRadar, setBarRadar] = useState(true);

  // 1. Fetch song data
  useEffect(() => {
    fetch(`http://${config.server_host}:${config.server_port}/song/${songId}`)
      .then(res => res.json())
      .then(data => setSongData(data))
      .catch(err => console.log(err));
  }, [songId]);

  // 2. Once songData is fetched, fetch the album
  useEffect(() => {
    if (songData?.album_id) {
      fetch(`http://${config.server_host}:${config.server_port}/album/${songData.album_id}`)
        .then(res => res.json())
        .then(data => setAlbumData(data))
        .catch(err => console.log(err));
    }
  }, [songData]);

  const chartData = [
    { name: 'Danceability', value: songData.danceability },
    { name: 'Energy', value: songData.energy },
    { name: 'Valence', value: songData.valence },
  ];

  const handleGraphChange = () => {
    setBarRadar(!barRadar);
  };

  return (
    <Modal
      open={true}
      onClose={handleClose}
      style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}
    >
      <Box
        p={3}
        style={{ background: 'white', borderRadius: '16px', border: '2px solid #000', width: 600 }}
      >
        <h1>{songData.title}</h1>
        <h2>Album:&nbsp;
          <NavLink to={`/albums/${albumData.album_id}`}>{albumData.title}</NavLink>
        </h2>
        <p>Duration: {formatDuration(songData.duration)}</p>
        <p>Tempo: {songData.tempo} bpm</p>
        <p>Key: {songData.key_mode}</p>
        <ButtonGroup>
          <Button disabled={barRadar} onClick={handleGraphChange}>Bar</Button>
          <Button disabled={!barRadar} onClick={handleGraphChange}>Radar</Button>
        </ButtonGroup>
        <div style={{ margin: 20 }}>
          { // This ternary statement returns a BarChart if barRadar is true, and a RadarChart otherwise
            barRadar
              ? (
                <ResponsiveContainer height={250}>
                  <BarChart
                    data={chartData}
                    layout='vertical'
                    margin={{ left: 40 }}
                  >
                    <XAxis type='number' domain={[0, 1]} />
                    <YAxis type='category' dataKey='name' />
                    <Bar dataKey='value' stroke='#8884d8' fill='#8884d8' />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <ResponsiveContainer height={250}>
                  <RadarChart
                    style={{ width: '100%', height: '100%', maxWidth: '500px', maxHeight: '80vh', aspectRatio: 1 }}
                    outerRadius="80%"
                    data={chartData}
                    margin={{ left: 40 }}
                  >
                    <PolarGrid />
                    <PolarAngleAxis dataKey="name" />
                    <PolarRadiusAxis />
                    <Radar
                      name="Attributes"
                      dataKey="value"
                      stroke="#8884d8"
                      fill="#8884d8"
                      fillOpacity={0.6}
                    />
                  </RadarChart>
                </ResponsiveContainer>
              )
          }
        </div>
        <Button onClick={handleClose} style={{ left: '50%', transform: 'translateX(-50%)' }} >
          Close
        </Button>
      </Box>
    </Modal>
  );
}

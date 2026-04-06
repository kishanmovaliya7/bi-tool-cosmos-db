const express = require('express');
const router = express.Router();

// Import main controller
const dashboardController = require('../controllers/dashboardController');

// Health check route
router.get('/', (req, res) => {
  res.json({
    message: 'Dashboard Visual Service is running',
    version: '1.0.0',
    endpoints: {
      process: 'POST /api/process'
    }
  });
});

// Main processing route
router.post('/process', dashboardController.getAllDashboards);

module.exports = { router };

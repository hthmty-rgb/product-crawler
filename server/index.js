/**
 * Supermarket Crawler - Express Server
 * Main entry point for the web application
 */

const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const archiver = require('archiver');
const { createObjectCsvWriter } = require('csv-writer');

const CrawlerDatabase = require('../database');
const SupermarketCrawler = require('../crawler');

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(morgan('dev'));
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// Initialize database
const dataDir = path.join(__dirname, '../data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new CrawlerDatabase(path.join(dataDir, 'database.sqlite'));

// Active crawlers map (jobId -> crawler instance)
const activeCrawlers = new Map();

// ==================== API ROUTES ====================

/**
 * GET /api/status
 * Get server status
 */
app.get('/api/status', (req, res) => {
  res.json({
    status: 'running',
    activeCrawls: activeCrawlers.size,
    totalProducts: db.getProductCount()
  });
});

/**
 * POST /api/crawl/start
 * Start a new crawl job
 */
app.post('/api/crawl/start', async (req, res) => {
  const { url, config = {} } = req.body;

  if (!url) {
    return res.status(400).json({ error: 'Homepage URL is required' });
  }

  // Validate URL
  try {
    new URL(url);
  } catch {
    return res.status(400).json({ error: 'Invalid URL format' });
  }

  // Create job ID
  const jobId = uuidv4();

  // Create job in database
  db.createJob(jobId, url, config);

  // Create crawler instance
  const crawler = new SupermarketCrawler(db, {
    ...config,
    imageDir: path.join(dataDir, 'images')
  });

  activeCrawlers.set(jobId, crawler);

  // Start crawl in background
  crawler.startCrawl(url, jobId)
    .then(() => {
      console.log(`[Server] Crawl ${jobId} completed`);
    })
    .catch(error => {
      console.error(`[Server] Crawl ${jobId} failed:`, error);
    })
    .finally(() => {
      activeCrawlers.delete(jobId);
    });

  res.json({
    success: true,
    jobId,
    message: 'Crawl started'
  });
});

/**
 * POST /api/crawl/stop/:jobId
 * Stop a running crawl
 */
app.post('/api/crawl/stop/:jobId', (req, res) => {
  const { jobId } = req.params;
  const crawler = activeCrawlers.get(jobId);

  if (!crawler) {
    return res.status(404).json({ error: 'Crawl job not found or already completed' });
  }

  crawler.stopCrawl();
  db.updateJobStatus(jobId, 'stopping');

  res.json({
    success: true,
    message: 'Stop signal sent'
  });
});

/**
 * GET /api/crawl/status/:jobId
 * Get status of a specific crawl job
 */
app.get('/api/crawl/status/:jobId', (req, res) => {
  const { jobId } = req.params;
  const job = db.getJob(jobId);

  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const crawler = activeCrawlers.get(jobId);
  const stats = crawler ? crawler.getStats() : null;

  res.json({
    ...job,
    liveStats: stats,
    errorLog: job.error_log ? JSON.parse(job.error_log) : []
  });
});

/**
 * GET /api/crawl/jobs
 * Get all crawl jobs
 */
app.get('/api/crawl/jobs', (req, res) => {
  const jobs = db.getAllJobs();
  res.json(jobs);
});

/**
 * GET /api/products
 * Get all products with optional filtering
 */
app.get('/api/products', (req, res) => {
  const { site, page = 1, limit = 50 } = req.query;
  
  let products = db.getAllProducts(site || null);
  
  // Pagination
  const start = (page - 1) * limit;
  const paginatedProducts = products.slice(start, start + parseInt(limit));
  
  res.json({
    total: products.length,
    page: parseInt(page),
    limit: parseInt(limit),
    products: paginatedProducts
  });
});

/**
 * GET /api/products/:productId
 * Get single product with images
 */
app.get('/api/products/:productId', (req, res) => {
  const { productId } = req.params;
  const product = db.getProduct(productId);

  if (!product) {
    return res.status(404).json({ error: 'Product not found' });
  }

  const images = db.getProductImages(productId);

  res.json({
    ...product,
    images
  });
});

/**
 * GET /api/barcodes
 * Get all detected barcodes
 */
app.get('/api/barcodes', (req, res) => {
  const images = db.getImagesWithBarcodes();
  
  // Group by barcode value
  const barcodeMap = {};
  for (const img of images) {
    if (!barcodeMap[img.barcode_value]) {
      barcodeMap[img.barcode_value] = {
        barcode: img.barcode_value,
        type: img.barcode_type,
        products: []
      };
    }
    barcodeMap[img.barcode_value].products.push({
      product_id: img.product_id,
      product_name: img.product_name,
      image_path: img.local_path
    });
  }

  res.json(Object.values(barcodeMap));
});

/**
 * GET /api/export/csv
 * Export products to CSV
 */
app.get('/api/export/csv', async (req, res) => {
  const products = db.getProductsForExport();
  
  const csvPath = path.join(dataDir, 'products.csv');
  
  const csvWriter = createObjectCsvWriter({
    path: csvPath,
    header: [
      { id: 'product_id', title: 'Product ID' },
      { id: 'name', title: 'Name' },
      { id: 'brand', title: 'Brand' },
      { id: 'category', title: 'Category' },
      { id: 'variant', title: 'Variant' },
      { id: 'price', title: 'Price' },
      { id: 'currency', title: 'Currency' },
      { id: 'description', title: 'Description' },
      { id: 'ingredients', title: 'Ingredients' },
      { id: 'nutrition', title: 'Nutrition' },
      { id: 'manufacturer', title: 'Manufacturer' },
      { id: 'origin', title: 'Origin' },
      { id: 'availability', title: 'Availability' },
      { id: 'barcodes', title: 'Barcodes' },
      { id: 'url', title: 'URL' },
      { id: 'source_site', title: 'Source Site' },
      { id: 'scrape_date', title: 'Scrape Date' },
      { id: 'image_paths', title: 'Image Paths' }
    ]
  });

  await csvWriter.writeRecords(products);

  res.download(csvPath, 'products.csv');
});

/**
 * GET /api/export/zip
 * Export images as ZIP
 */
app.get('/api/export/zip', (req, res) => {
  const imagesDir = path.join(dataDir, 'images');
  
  if (!fs.existsSync(imagesDir)) {
    return res.status(404).json({ error: 'No images found' });
  }

  res.attachment('images.zip');
  
  const archive = archiver('zip', { zlib: { level: 5 } });
  archive.pipe(res);
  archive.directory(imagesDir, 'images');
  archive.finalize();
});

/**
 * GET /api/stats
 * Get overall statistics
 */
app.get('/api/stats', (req, res) => {
  const products = db.getAllProducts();
  const jobs = db.getAllJobs();
  const barcodes = db.getImagesWithBarcodes();

  // Count by source site
  const siteStats = {};
  for (const product of products) {
    const site = product.source_site || 'unknown';
    siteStats[site] = (siteStats[site] || 0) + 1;
  }

  res.json({
    totalProducts: products.length,
    totalJobs: jobs.length,
    completedJobs: jobs.filter(j => j.status === 'completed').length,
    totalBarcodes: barcodes.length,
    uniqueBarcodes: new Set(barcodes.map(b => b.barcode_value)).size,
    productsBySite: siteStats
  });
});

/**
 * Serve images
 */
app.use('/images', express.static(path.join(dataDir, 'images')));

/**
 * Serve database file
 */
app.get('/api/export/database', (req, res) => {
  const dbPath = path.join(dataDir, 'database.sqlite');
  res.download(dbPath, 'database.sqlite');
});

// ==================== ERROR HANDLING ====================

app.use((err, req, res, next) => {
  console.error('Server error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// ==================== SERVER START ====================

const server = app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════════╗
║          SUPERMARKET PRODUCT CRAWLER                       ║
║                                                            ║
║  Server running on port ${PORT}                              ║
║  Dashboard: http://localhost:${PORT}                         ║
║                                                            ║
║  API Endpoints:                                            ║
║  - POST /api/crawl/start     Start new crawl              ║
║  - POST /api/crawl/stop/:id  Stop crawl                   ║
║  - GET  /api/crawl/status/:id Get crawl status            ║
║  - GET  /api/products        List products                ║
║  - GET  /api/export/csv      Export CSV                   ║
║  - GET  /api/export/zip      Export images ZIP            ║
╚════════════════════════════════════════════════════════════╝
  `);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received. Shutting down...');
  
  // Stop all active crawlers
  for (const [jobId, crawler] of activeCrawlers) {
    crawler.stopCrawl();
    db.updateJobStatus(jobId, 'stopped');
  }
  
  server.close(() => {
    db.close();
    process.exit(0);
  });
});

module.exports = app;

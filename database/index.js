/**
 * Database Module
 * Handles all SQLite operations for the supermarket crawler
 * Uses better-sqlite3 for synchronous, fast operations
 */

const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class CrawlerDatabase {
  constructor(dbPath = './data/database.sqlite') {
    // Ensure data directory exists
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL'); // Better concurrent access
    this.initTables();
  }

  /**
   * Initialize database tables
   */
  initTables() {
    // Products table - main product information
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        name TEXT,
        brand TEXT,
        category TEXT,
        variant TEXT,
        price REAL,
        currency TEXT,
        description TEXT,
        ingredients TEXT,
        nutrition TEXT,
        manufacturer TEXT,
        origin TEXT,
        availability TEXT,
        scrape_date TEXT,
        source_site TEXT,
        merged_ocr_text TEXT,
        ocr_fields_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Images table - product images with barcode and OCR data
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT NOT NULL,
        image_url TEXT,
        local_path TEXT,
        tag TEXT DEFAULT 'other',
        barcode_value TEXT,
        barcode_type TEXT,
        barcode_confidence REAL,
        ocr_text TEXT,
        ocr_confidence REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
      )
    `);

    // Crawl jobs table - tracking crawl sessions
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS crawl_jobs (
        job_id TEXT PRIMARY KEY,
        homepage_url TEXT,
        status TEXT DEFAULT 'pending',
        started_at TEXT,
        finished_at TEXT,
        total_categories INTEGER DEFAULT 0,
        processed_categories INTEGER DEFAULT 0,
        total_products INTEGER DEFAULT 0,
        processed_products INTEGER DEFAULT 0,
        errors INTEGER DEFAULT 0,
        error_log TEXT,
        config_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Categories table - discovered categories
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        url TEXT,
        name TEXT,
        parent_url TEXT,
        depth INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        product_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (job_id) REFERENCES crawl_jobs(job_id)
      )
    `);

    // URL queue table - for resumable crawling
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS url_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        url TEXT,
        type TEXT,
        status TEXT DEFAULT 'pending',
        retry_count INTEGER DEFAULT 0,
        last_error TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (job_id) REFERENCES crawl_jobs(job_id)
      )
    `);

    // Create indexes for performance
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_products_source ON products(source_site);
      CREATE INDEX IF NOT EXISTS idx_images_product ON images(product_id);
      CREATE INDEX IF NOT EXISTS idx_images_barcode ON images(barcode_value);
      CREATE INDEX IF NOT EXISTS idx_categories_job ON categories(job_id);
      CREATE INDEX IF NOT EXISTS idx_queue_job_status ON url_queue(job_id, status);
    `);
  }

  // ==================== CRAWL JOBS ====================

  createJob(jobId, homepageUrl, config = {}) {
    const stmt = this.db.prepare(`
      INSERT INTO crawl_jobs (job_id, homepage_url, status, config_json, started_at)
      VALUES (?, ?, 'running', ?, datetime('now'))
    `);
    stmt.run(jobId, homepageUrl, JSON.stringify(config));
    return jobId;
  }

  getJob(jobId) {
    const stmt = this.db.prepare('SELECT * FROM crawl_jobs WHERE job_id = ?');
    return stmt.get(jobId);
  }

  getAllJobs() {
    const stmt = this.db.prepare('SELECT * FROM crawl_jobs ORDER BY created_at DESC');
    return stmt.all();
  }

  updateJobStatus(jobId, status, additionalFields = {}) {
    let updates = ['status = ?'];
    let values = [status];

    if (status === 'completed' || status === 'failed' || status === 'stopped') {
      updates.push("finished_at = datetime('now')");
    }

    for (const [key, value] of Object.entries(additionalFields)) {
      updates.push(`${key} = ?`);
      values.push(value);
    }

    values.push(jobId);
    const stmt = this.db.prepare(`
      UPDATE crawl_jobs SET ${updates.join(', ')} WHERE job_id = ?
    `);
    stmt.run(...values);
  }

  incrementJobCounter(jobId, field, amount = 1) {
    const stmt = this.db.prepare(`
      UPDATE crawl_jobs SET ${field} = ${field} + ? WHERE job_id = ?
    `);
    stmt.run(amount, jobId);
  }

  appendJobError(jobId, error) {
    const job = this.getJob(jobId);
    const errors = job?.error_log ? JSON.parse(job.error_log) : [];
    errors.push({
      timestamp: new Date().toISOString(),
      error: error.toString()
    });
    // Keep only last 100 errors
    const trimmed = errors.slice(-100);
    const stmt = this.db.prepare(`
      UPDATE crawl_jobs SET error_log = ?, errors = errors + 1 WHERE job_id = ?
    `);
    stmt.run(JSON.stringify(trimmed), jobId);
  }

  // ==================== PRODUCTS ====================

  upsertProduct(product) {
    const stmt = this.db.prepare(`
      INSERT INTO products (
        product_id, url, name, brand, category, variant,
        price, currency, description, ingredients, nutrition,
        manufacturer, origin, availability, scrape_date, source_site,
        merged_ocr_text, ocr_fields_json
      ) VALUES (
        @product_id, @url, @name, @brand, @category, @variant,
        @price, @currency, @description, @ingredients, @nutrition,
        @manufacturer, @origin, @availability, @scrape_date, @source_site,
        @merged_ocr_text, @ocr_fields_json
      )
      ON CONFLICT(product_id) DO UPDATE SET
        name = excluded.name,
        brand = excluded.brand,
        category = excluded.category,
        variant = excluded.variant,
        price = excluded.price,
        currency = excluded.currency,
        description = excluded.description,
        ingredients = excluded.ingredients,
        nutrition = excluded.nutrition,
        manufacturer = excluded.manufacturer,
        origin = excluded.origin,
        availability = excluded.availability,
        scrape_date = excluded.scrape_date,
        merged_ocr_text = excluded.merged_ocr_text,
        ocr_fields_json = excluded.ocr_fields_json,
        updated_at = datetime('now')
    `);

    stmt.run({
      product_id: product.product_id,
      url: product.url,
      name: product.name || null,
      brand: product.brand || null,
      category: product.category || null,
      variant: product.variant || null,
      price: product.price || null,
      currency: product.currency || null,
      description: product.description || null,
      ingredients: product.ingredients || null,
      nutrition: product.nutrition || null,
      manufacturer: product.manufacturer || null,
      origin: product.origin || null,
      availability: product.availability || null,
      scrape_date: product.scrape_date || new Date().toISOString(),
      source_site: product.source_site || null,
      merged_ocr_text: product.merged_ocr_text || null,
      ocr_fields_json: product.ocr_fields_json ? JSON.stringify(product.ocr_fields_json) : null
    });
  }

  getProduct(productId) {
    const stmt = this.db.prepare('SELECT * FROM products WHERE product_id = ?');
    return stmt.get(productId);
  }

  getProductByUrl(url) {
    const stmt = this.db.prepare('SELECT * FROM products WHERE url = ?');
    return stmt.get(url);
  }

  getAllProducts(sourceSite = null) {
    if (sourceSite) {
      const stmt = this.db.prepare('SELECT * FROM products WHERE source_site = ?');
      return stmt.all(sourceSite);
    }
    const stmt = this.db.prepare('SELECT * FROM products');
    return stmt.all();
  }

  getProductCount(sourceSite = null) {
    if (sourceSite) {
      const stmt = this.db.prepare('SELECT COUNT(*) as count FROM products WHERE source_site = ?');
      return stmt.get(sourceSite).count;
    }
    const stmt = this.db.prepare('SELECT COUNT(*) as count FROM products');
    return stmt.get().count;
  }

  productExists(productId) {
    const stmt = this.db.prepare('SELECT 1 FROM products WHERE product_id = ? LIMIT 1');
    return !!stmt.get(productId);
  }

  // ==================== IMAGES ====================

  addImage(image) {
    const stmt = this.db.prepare(`
      INSERT INTO images (
        product_id, image_url, local_path, tag,
        barcode_value, barcode_type, barcode_confidence,
        ocr_text, ocr_confidence
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const result = stmt.run(
      image.product_id,
      image.image_url || null,
      image.local_path || null,
      image.tag || 'other',
      image.barcode_value || null,
      image.barcode_type || null,
      image.barcode_confidence || null,
      image.ocr_text || null,
      image.ocr_confidence || null
    );

    return result.lastInsertRowid;
  }

  updateImageBarcode(imageId, barcodeValue, barcodeType, confidence) {
    const stmt = this.db.prepare(`
      UPDATE images SET
        barcode_value = ?,
        barcode_type = ?,
        barcode_confidence = ?
      WHERE id = ?
    `);
    stmt.run(barcodeValue, barcodeType, confidence, imageId);
  }

  updateImageOcr(imageId, ocrText, confidence) {
    const stmt = this.db.prepare(`
      UPDATE images SET ocr_text = ?, ocr_confidence = ? WHERE id = ?
    `);
    stmt.run(ocrText, confidence, imageId);
  }

  getProductImages(productId) {
    const stmt = this.db.prepare('SELECT * FROM images WHERE product_id = ?');
    return stmt.all(productId);
  }

  imageExists(productId, imageUrl) {
    const stmt = this.db.prepare(
      'SELECT 1 FROM images WHERE product_id = ? AND image_url = ? LIMIT 1'
    );
    return !!stmt.get(productId, imageUrl);
  }

  getImagesWithBarcodes() {
    const stmt = this.db.prepare(`
      SELECT i.*, p.name as product_name
      FROM images i
      JOIN products p ON i.product_id = p.product_id
      WHERE i.barcode_value IS NOT NULL
    `);
    return stmt.all();
  }

  // ==================== CATEGORIES ====================

  addCategory(jobId, url, name, parentUrl = null, depth = 0) {
    const stmt = this.db.prepare(`
      INSERT OR IGNORE INTO categories (job_id, url, name, parent_url, depth)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run(jobId, url, name, parentUrl, depth);
  }

  getCategories(jobId) {
    const stmt = this.db.prepare('SELECT * FROM categories WHERE job_id = ?');
    return stmt.all(jobId);
  }

  getPendingCategories(jobId) {
    const stmt = this.db.prepare(`
      SELECT * FROM categories WHERE job_id = ? AND status = 'pending'
    `);
    return stmt.all(jobId);
  }

  updateCategoryStatus(categoryId, status, productCount = null) {
    if (productCount !== null) {
      const stmt = this.db.prepare(`
        UPDATE categories SET status = ?, product_count = ? WHERE id = ?
      `);
      stmt.run(status, productCount, categoryId);
    } else {
      const stmt = this.db.prepare('UPDATE categories SET status = ? WHERE id = ?');
      stmt.run(status, categoryId);
    }
  }

  // ==================== URL QUEUE ====================

  addToQueue(jobId, url, type = 'product') {
    const stmt = this.db.prepare(`
      INSERT OR IGNORE INTO url_queue (job_id, url, type)
      VALUES (?, ?, ?)
    `);
    stmt.run(jobId, url, type);
  }

  getNextFromQueue(jobId, limit = 10) {
    const stmt = this.db.prepare(`
      SELECT * FROM url_queue
      WHERE job_id = ? AND status = 'pending' AND retry_count < 3
      ORDER BY created_at ASC
      LIMIT ?
    `);
    return stmt.all(jobId, limit);
  }

  updateQueueItem(id, status, error = null) {
    if (status === 'failed' && error) {
      const stmt = this.db.prepare(`
        UPDATE url_queue SET status = ?, last_error = ?, retry_count = retry_count + 1
        WHERE id = ?
      `);
      stmt.run(status, error, id);
    } else {
      const stmt = this.db.prepare('UPDATE url_queue SET status = ? WHERE id = ?');
      stmt.run(status, id);
    }
  }

  resetFailedQueueItems(jobId) {
    const stmt = this.db.prepare(`
      UPDATE url_queue SET status = 'pending'
      WHERE job_id = ? AND status = 'failed' AND retry_count < 3
    `);
    stmt.run(jobId);
  }

  // ==================== EXPORT ====================

  getProductsForExport() {
    const stmt = this.db.prepare(`
      SELECT
        p.*,
        GROUP_CONCAT(DISTINCT i.barcode_value) as barcodes,
        GROUP_CONCAT(DISTINCT i.local_path) as image_paths
      FROM products p
      LEFT JOIN images i ON p.product_id = i.product_id
      GROUP BY p.product_id
    `);
    return stmt.all();
  }

  // ==================== CLEANUP ====================

  close() {
    this.db.close();
  }

  vacuum() {
    this.db.exec('VACUUM');
  }
}

module.exports = CrawlerDatabase;

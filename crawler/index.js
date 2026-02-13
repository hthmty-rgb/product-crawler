/**
 * Supermarket Crawler Module
 * Uses Playwright for headless browser automation
 * Handles category discovery, pagination, and product extraction
 */

const { chromium } = require('playwright');
const cheerio = require('cheerio');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));
const sharp = require('sharp');
const xml2js = require('xml2js');

const CrawlerDatabase = require('../database');
const { detectBarcode, extractBarcodeFromOCR } = require('../barcode');
const { extractText, classifyImageContent, parseOCRFields } = require('../ocr');

// Default configuration
const DEFAULT_CONFIG = {
  concurrency: 2,
  requestDelay: 1000, // ms between requests
  pageTimeout: 30000, // 30 seconds
  maxRetries: 3,
  maxDepth: 5,
  imageDir: './data/images',
  enableOCR: true,
  enableBarcode: true,
  userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
};

/**
 * Main Crawler Class
 */
class SupermarketCrawler {
  constructor(db, config = {}) {
    this.db = db;
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.browser = null;
    this.context = null;
    this.jobId = null;
    this.running = false;
    this.stopRequested = false;
    this.processedUrls = new Set();
    this.discoveredProducts = new Set();
    this.stats = {
      categories: 0,
      products: 0,
      images: 0,
      barcodes: 0,
      errors: 0
    };
  }

  /**
   * Generate stable product ID
   * Priority: SKU from page > SHA1 of URL
   */
  generateProductId(url, sku = null) {
    if (sku) {
      // Sanitize SKU
      return sku.replace(/[^a-zA-Z0-9]/g, '').substring(0, 12).toUpperCase();
    }
    // Use SHA1 hash of URL
    const hash = crypto.createHash('sha1').update(url).digest('hex');
    return hash.substring(0, 12).toUpperCase();
  }

  /**
   * Initialize browser
   */
  async initBrowser() {
    this.browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu'
      ]
    });

    this.context = await this.browser.newContext({
      userAgent: this.config.userAgent,
      viewport: { width: 1920, height: 1080 },
      ignoreHTTPSErrors: true
    });

    // Block unnecessary resources
    await this.context.route('**/*', (route) => {
      const resourceType = route.request().resourceType();
      if (['font', 'media'].includes(resourceType)) {
        route.abort();
      } else {
        route.continue();
      }
    });
  }

  /**
   * Close browser
   */
  async closeBrowser() {
    if (this.context) await this.context.close();
    if (this.browser) await this.browser.close();
    this.browser = null;
    this.context = null;
  }

  /**
   * Extract base domain from URL
   */
  getBaseDomain(url) {
    try {
      const urlObj = new URL(url);
      return urlObj.origin;
    } catch {
      return url;
    }
  }

  /**
   * Normalize URL
   */
  normalizeUrl(url, baseUrl) {
    try {
      if (url.startsWith('//')) {
        url = 'https:' + url;
      }
      if (url.startsWith('/')) {
        const base = new URL(baseUrl);
        return base.origin + url;
      }
      if (!url.startsWith('http')) {
        return new URL(url, baseUrl).href;
      }
      return url;
    } catch {
      return null;
    }
  }

  /**
   * Delay helper
   */
  async delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms || this.config.requestDelay));
  }

  /**
   * Start crawl job
   */
  async startCrawl(homepageUrl, jobId) {
    this.jobId = jobId;
    this.running = true;
    this.stopRequested = false;
    this.processedUrls.clear();
    this.discoveredProducts.clear();
    this.stats = { categories: 0, products: 0, images: 0, barcodes: 0, errors: 0 };

    const baseDomain = this.getBaseDomain(homepageUrl);

    try {
      await this.initBrowser();
      console.log(`[Crawler] Starting crawl of ${homepageUrl}`);

      // Step 1: Discover categories
      const categories = await this.discoverCategories(homepageUrl);
      console.log(`[Crawler] Discovered ${categories.length} categories`);

      this.db.updateJobStatus(this.jobId, 'running', {
        total_categories: categories.length
      });

      // Step 2: Crawl each category
      for (const category of categories) {
        if (this.stopRequested) break;

        try {
          await this.crawlCategory(category, baseDomain);
          this.db.incrementJobCounter(this.jobId, 'processed_categories');
          this.stats.categories++;
        } catch (error) {
          console.error(`[Crawler] Category error: ${error.message}`);
          this.db.appendJobError(this.jobId, error.message);
          this.stats.errors++;
        }

        await this.delay();
      }

      // Mark job complete
      const status = this.stopRequested ? 'stopped' : 'completed';
      this.db.updateJobStatus(this.jobId, status, {
        total_products: this.stats.products,
        processed_products: this.stats.products
      });

    } catch (error) {
      console.error(`[Crawler] Fatal error: ${error.message}`);
      this.db.updateJobStatus(this.jobId, 'failed');
      this.db.appendJobError(this.jobId, error.message);
      throw error;
    } finally {
      await this.closeBrowser();
      this.running = false;
    }

    return this.stats;
  }

  /**
   * Stop crawl
   */
  stopCrawl() {
    this.stopRequested = true;
    console.log('[Crawler] Stop requested');
  }

  /**
   * Discover categories from homepage
   */
  async discoverCategories(homepageUrl) {
    const categories = [];
    const baseDomain = this.getBaseDomain(homepageUrl);

    // Try sitemap first
    const sitemapCategories = await this.parseSitemap(baseDomain);
    if (sitemapCategories.length > 0) {
      console.log(`[Crawler] Found ${sitemapCategories.length} URLs from sitemap`);
      categories.push(...sitemapCategories);
    }

    // Try navigation menu
    const page = await this.context.newPage();
    try {
      await page.goto(homepageUrl, { 
        waitUntil: 'networkidle',
        timeout: this.config.pageTimeout
      });

      // Wait for any dynamic content
      await page.waitForTimeout(2000);

      // Extract navigation links
      const navCategories = await this.extractNavigationLinks(page, baseDomain);
      console.log(`[Crawler] Found ${navCategories.length} navigation links`);

      // Merge and deduplicate
      for (const cat of navCategories) {
        if (!categories.find(c => c.url === cat.url)) {
          categories.push(cat);
        }
      }

      // Store categories in database
      for (const cat of categories) {
        this.db.addCategory(this.jobId, cat.url, cat.name);
      }

    } finally {
      await page.close();
    }

    return categories;
  }

  /**
   * Parse sitemap.xml
   */
  async parseSitemap(baseDomain) {
    const categories = [];
    const sitemapUrls = [
      `${baseDomain}/sitemap.xml`,
      `${baseDomain}/sitemap_index.xml`,
      `${baseDomain}/sitemap-index.xml`
    ];

    for (const sitemapUrl of sitemapUrls) {
      try {
        const response = await fetch(sitemapUrl, {
          headers: { 'User-Agent': this.config.userAgent },
          timeout: 10000
        });

        if (!response.ok) continue;

        const xml = await response.text();
        const parser = new xml2js.Parser();
        const result = await parser.parseStringPromise(xml);

        // Handle sitemap index
        if (result.sitemapindex) {
          const sitemaps = result.sitemapindex.sitemap || [];
          for (const sm of sitemaps) {
            const loc = sm.loc?.[0];
            if (loc && (loc.includes('product') || loc.includes('category'))) {
              const subCategories = await this.parseSitemap(loc);
              categories.push(...subCategories);
            }
          }
        }

        // Handle urlset
        if (result.urlset) {
          const urls = result.urlset.url || [];
          for (const url of urls) {
            const loc = url.loc?.[0];
            if (loc && this.isCategoryUrl(loc)) {
              categories.push({
                url: loc,
                name: this.extractNameFromUrl(loc),
                source: 'sitemap'
              });
            }
          }
        }

        if (categories.length > 0) break;
      } catch (error) {
        // Continue to next sitemap URL
      }
    }

    return categories;
  }

  /**
   * Check if URL looks like a category
   */
  isCategoryUrl(url) {
    const categoryPatterns = [
      /\/category\//i,
      /\/categories\//i,
      /\/c\//i,
      /\/collections?\//i,
      /\/shop\//i,
      /\/products?\//i,
      /\/department\//i,
      /\/aisle\//i
    ];

    return categoryPatterns.some(p => p.test(url));
  }

  /**
   * Check if URL looks like a product
   */
  isProductUrl(url) {
    const productPatterns = [
      /\/product\//i,
      /\/p\//i,
      /\/item\//i,
      /\/pd\//i,
      /\/dp\//i,
      /\?sku=/i,
      /\?id=/i,
      /\/\d{5,}/  // Long numeric ID
    ];

    return productPatterns.some(p => p.test(url));
  }

  /**
   * Extract name from URL path
   */
  extractNameFromUrl(url) {
    try {
      const urlObj = new URL(url);
      const parts = urlObj.pathname.split('/').filter(p => p);
      const last = parts[parts.length - 1];
      return last?.replace(/[-_]/g, ' ').replace(/\d+/g, '').trim() || 'Unknown';
    } catch {
      return 'Unknown';
    }
  }

  /**
   * Extract navigation links from page
   */
  async extractNavigationLinks(page, baseDomain) {
    const links = await page.evaluate(() => {
      const results = [];
      const selectors = [
        'nav a',
        '.nav a',
        '.menu a',
        '.navigation a',
        '[class*="category"] a',
        '[class*="menu"] a',
        'header a',
        '.header a'
      ];

      for (const selector of selectors) {
        const elements = document.querySelectorAll(selector);
        elements.forEach(el => {
          const href = el.href;
          const text = el.textContent?.trim();
          if (href && text && href.startsWith('http')) {
            results.push({ url: href, name: text });
          }
        });
      }

      return results;
    });

    // Filter to same domain and category-like URLs
    return links.filter(link => {
      try {
        const linkDomain = new URL(link.url).origin;
        return linkDomain === baseDomain && 
               (this.isCategoryUrl(link.url) || link.name.length > 2);
      } catch {
        return false;
      }
    });
  }

  /**
   * Crawl a single category
   */
  async crawlCategory(category, baseDomain) {
    console.log(`[Crawler] Crawling category: ${category.name}`);
    
    const page = await this.context.newPage();
    let pageNum = 1;
    let hasMorePages = true;

    try {
      while (hasMorePages && !this.stopRequested) {
        const pageUrl = this.getPaginatedUrl(category.url, pageNum);
        
        if (this.processedUrls.has(pageUrl)) {
          break;
        }
        this.processedUrls.add(pageUrl);

        console.log(`[Crawler] Loading page ${pageNum}: ${pageUrl}`);
        
        await page.goto(pageUrl, {
          waitUntil: 'networkidle',
          timeout: this.config.pageTimeout
        });

        // Handle infinite scroll
        await this.handleInfiniteScroll(page);

        // Extract product links
        const productUrls = await this.extractProductLinks(page, baseDomain);
        console.log(`[Crawler] Found ${productUrls.length} products on page ${pageNum}`);

        if (productUrls.length === 0) {
          hasMorePages = false;
          continue;
        }

        // Process each product
        for (const productUrl of productUrls) {
          if (this.stopRequested) break;
          if (this.discoveredProducts.has(productUrl)) continue;
          this.discoveredProducts.add(productUrl);

          try {
            await this.crawlProduct(productUrl, category.name, baseDomain);
            this.stats.products++;
            this.db.incrementJobCounter(this.jobId, 'processed_products');
          } catch (error) {
            console.error(`[Crawler] Product error: ${error.message}`);
            this.db.appendJobError(this.jobId, `${productUrl}: ${error.message}`);
            this.stats.errors++;
          }

          await this.delay();
        }

        // Check for next page
        hasMorePages = await this.hasNextPage(page);
        pageNum++;
      }
    } finally {
      await page.close();
    }
  }

  /**
   * Get paginated URL
   */
  getPaginatedUrl(url, pageNum) {
    if (pageNum === 1) return url;
    
    const urlObj = new URL(url);
    
    // Try common pagination patterns
    if (urlObj.searchParams.has('page')) {
      urlObj.searchParams.set('page', pageNum);
    } else if (urlObj.searchParams.has('p')) {
      urlObj.searchParams.set('p', pageNum);
    } else {
      urlObj.searchParams.set('page', pageNum);
    }
    
    return urlObj.href;
  }

  /**
   * Handle infinite scroll pages
   */
  async handleInfiniteScroll(page) {
    let previousHeight = 0;
    let scrollAttempts = 0;
    const maxScrolls = 10;

    while (scrollAttempts < maxScrolls) {
      const currentHeight = await page.evaluate(() => document.body.scrollHeight);
      
      if (currentHeight === previousHeight) break;
      
      previousHeight = currentHeight;
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1000);
      scrollAttempts++;
    }
  }

  /**
   * Check if there's a next page
   */
  async hasNextPage(page) {
    return await page.evaluate(() => {
      const nextSelectors = [
        'a[rel="next"]',
        '.pagination .next:not(.disabled)',
        '[class*="next"]:not(.disabled) a',
        'a:has-text("Next")',
        'a:has-text(">")'
      ];

      for (const selector of nextSelectors) {
        const el = document.querySelector(selector);
        if (el && !el.classList.contains('disabled')) {
          return true;
        }
      }
      return false;
    });
  }

  /**
   * Extract product links from category page
   */
  async extractProductLinks(page, baseDomain) {
    const links = await page.evaluate(() => {
      const results = [];
      const selectors = [
        '[class*="product"] a',
        '[class*="item"] a',
        '.product-card a',
        '.product-tile a',
        '[data-product] a',
        'article a',
        '.grid a'
      ];

      const seen = new Set();

      for (const selector of selectors) {
        const elements = document.querySelectorAll(selector);
        elements.forEach(el => {
          const href = el.href;
          if (href && !seen.has(href)) {
            seen.add(href);
            results.push(href);
          }
        });
      }

      return results;
    });

    // Filter to same domain and product-like URLs
    return links.filter(url => {
      try {
        const linkDomain = new URL(url).origin;
        return linkDomain === baseDomain && this.isProductUrl(url);
      } catch {
        return false;
      }
    });
  }

  /**
   * Crawl a single product page
   */
  async crawlProduct(productUrl, category, baseDomain) {
    console.log(`[Crawler] Processing product: ${productUrl}`);

    const page = await this.context.newPage();
    
    try {
      // Intercept JSON responses for structured data
      let jsonData = null;
      page.on('response', async response => {
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('application/json')) {
          try {
            const json = await response.json();
            if (json.product || json.item || json.data) {
              jsonData = json;
            }
          } catch {}
        }
      });

      await page.goto(productUrl, {
        waitUntil: 'networkidle',
        timeout: this.config.pageTimeout
      });

      // Extract product data
      const productData = await this.extractProductData(page, productUrl, category, baseDomain, jsonData);
      
      // Save product to database
      this.db.upsertProduct(productData);

      // Download and process images
      if (productData.images && productData.images.length > 0) {
        await this.processProductImages(productData.product_id, productData.images);
      }

    } finally {
      await page.close();
    }
  }

  /**
   * Extract product data from page
   */
  async extractProductData(page, productUrl, category, baseDomain, jsonData = null) {
    // Try JSON-LD structured data first
    let structuredData = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (const script of scripts) {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'Product' || data.product) {
            return data.product || data;
          }
        } catch {}
      }
      return null;
    });

    // Merge with intercepted JSON if available
    if (jsonData) {
      structuredData = { ...structuredData, ...jsonData.product, ...jsonData.item };
    }

    // Extract from DOM as fallback
    const domData = await page.evaluate(() => {
      const getText = (selectors) => {
        for (const sel of selectors) {
          const el = document.querySelector(sel);
          if (el) return el.textContent?.trim();
        }
        return null;
      };

      const getImages = () => {
        const images = [];
        const selectors = [
          '[class*="product"] img',
          '[class*="gallery"] img',
          '.product-image img',
          '[data-zoom]',
          'img[src*="product"]'
        ];
        
        const seen = new Set();
        for (const sel of selectors) {
          document.querySelectorAll(sel).forEach(img => {
            const src = img.src || img.dataset.src || img.dataset.zoom;
            if (src && !seen.has(src) && !src.includes('icon') && !src.includes('logo')) {
              seen.add(src);
              images.push(src);
            }
          });
        }
        return images;
      };

      return {
        name: getText([
          'h1',
          '[class*="product-name"]',
          '[class*="product-title"]',
          '[itemprop="name"]'
        ]),
        brand: getText([
          '[itemprop="brand"]',
          '[class*="brand"]',
          '.manufacturer'
        ]),
        price: getText([
          '[class*="price"]:not([class*="was"])',
          '[itemprop="price"]',
          '.current-price'
        ]),
        description: getText([
          '[itemprop="description"]',
          '[class*="description"]',
          '.product-description'
        ]),
        sku: getText([
          '[itemprop="sku"]',
          '[class*="sku"]',
          '[data-sku]'
        ]) || document.querySelector('[data-sku]')?.dataset.sku,
        images: getImages(),
        breadcrumb: Array.from(document.querySelectorAll('[class*="breadcrumb"] a, nav[aria-label="breadcrumb"] a'))
          .map(a => a.textContent?.trim())
          .filter(t => t)
          .join(' > ')
      };
    });

    // Merge data sources
    const merged = {
      ...domData,
      ...structuredData
    };

    // Parse price
    let price = null;
    let currency = null;
    if (merged.price) {
      const priceMatch = merged.price.match(/([A-Z]{3}|\$|€|£|¥|﷼)?[\s]*(\d+[.,]?\d*)/);
      if (priceMatch) {
        price = parseFloat(priceMatch[2].replace(',', '.'));
        currency = priceMatch[1] || 'USD';
      }
    }

    // Generate stable product ID
    const productId = this.generateProductId(productUrl, merged.sku);

    return {
      product_id: productId,
      url: productUrl,
      name: merged.name || merged.title,
      brand: merged.brand?.name || merged.brand,
      category: merged.breadcrumb || category,
      variant: merged.variant || merged.size,
      price: price,
      currency: currency,
      description: merged.description,
      ingredients: merged.ingredients,
      nutrition: merged.nutrition,
      manufacturer: merged.manufacturer,
      origin: merged.origin || merged.countryOfOrigin,
      availability: merged.availability || merged.offers?.availability,
      scrape_date: new Date().toISOString(),
      source_site: baseDomain,
      images: merged.images || []
    };
  }

  /**
   * Process and save product images
   */
  async processProductImages(productId, imageUrls) {
    const imageDir = path.join(this.config.imageDir, productId);
    
    if (!fs.existsSync(imageDir)) {
      fs.mkdirSync(imageDir, { recursive: true });
    }

    let ocrTexts = [];
    let ocrFields = {};

    for (let i = 0; i < imageUrls.length && i < 10; i++) {
      const imageUrl = imageUrls[i];
      
      // Skip if already processed
      if (this.db.imageExists(productId, imageUrl)) {
        continue;
      }

      try {
        // Download image
        const response = await fetch(imageUrl, {
          headers: { 'User-Agent': this.config.userAgent }
        });

        if (!response.ok) continue;

        const buffer = Buffer.from(await response.arrayBuffer());

        // Classify image and process
        let tag = 'other';
        let barcodeResult = null;
        let ocrResult = null;

        // Try barcode detection
        if (this.config.enableBarcode) {
          barcodeResult = await detectBarcode(buffer);
          if (barcodeResult) {
            tag = 'barcode';
            this.stats.barcodes++;
          }
        }

        // Run OCR if enabled
        if (this.config.enableOCR) {
          ocrResult = await extractText(buffer);
          if (ocrResult.text) {
            tag = classifyImageContent(ocrResult.text);
            ocrTexts.push(ocrResult.text);
            
            const fields = parseOCRFields(ocrResult.text);
            ocrFields = { ...ocrFields, ...fields };

            // Try to extract barcode from OCR if not found
            if (!barcodeResult && ocrResult.text) {
              const ocrBarcode = extractBarcodeFromOCR(ocrResult.text);
              if (ocrBarcode) {
                barcodeResult = {
                  value: ocrBarcode,
                  format: ocrBarcode.length === 13 ? 'EAN-13' : 'Unknown',
                  confidence: 0.5
                };
              }
            }
          }
        }

        // Determine filename
        const indexStr = String(i + 1).padStart(2, '0');
        const filename = `${productId}__img${indexStr}__${tag}.jpg`;
        const localPath = path.join(imageDir, filename);

        // Save image (convert to JPEG)
        await sharp(buffer)
          .jpeg({ quality: 85 })
          .toFile(localPath);

        // Save to database
        this.db.addImage({
          product_id: productId,
          image_url: imageUrl,
          local_path: localPath,
          tag: tag,
          barcode_value: barcodeResult?.value,
          barcode_type: barcodeResult?.format,
          barcode_confidence: barcodeResult?.confidence,
          ocr_text: ocrResult?.text,
          ocr_confidence: ocrResult?.confidence
        });

        this.stats.images++;

      } catch (error) {
        console.error(`[Crawler] Image error: ${error.message}`);
      }

      await this.delay(200); // Small delay between images
    }

    // Update product with merged OCR data
    if (ocrTexts.length > 0) {
      const product = this.db.getProduct(productId);
      if (product) {
        this.db.upsertProduct({
          ...product,
          merged_ocr_text: ocrTexts.join('\n---\n'),
          ocr_fields_json: ocrFields
        });
      }
    }
  }

  /**
   * Get current stats
   */
  getStats() {
    return {
      ...this.stats,
      running: this.running,
      jobId: this.jobId
    };
  }
}

module.exports = SupermarketCrawler;

/**
 * OCR Module
 * Uses Tesseract.js for text extraction from product images
 */

const Tesseract = require('tesseract.js');
const sharp = require('sharp');
const path = require('path');
const fs = require('fs');

// Tesseract worker pool
let scheduler = null;
const MAX_WORKERS = 2;

/**
 * Initialize Tesseract scheduler with workers
 */
async function initScheduler() {
  if (scheduler) return scheduler;

  scheduler = Tesseract.createScheduler();
  
  // Create workers
  for (let i = 0; i < MAX_WORKERS; i++) {
    const worker = await Tesseract.createWorker('eng', 1, {
      logger: () => {} // Silent logging
    });
    scheduler.addWorker(worker);
  }

  console.log(`OCR: Initialized ${MAX_WORKERS} Tesseract workers`);
  return scheduler;
}

/**
 * Terminate scheduler and workers
 */
async function terminateScheduler() {
  if (scheduler) {
    await scheduler.terminate();
    scheduler = null;
  }
}

/**
 * Preprocess image for OCR
 * @param {Buffer} imageBuffer - Original image
 * @returns {Promise<Buffer>} - Preprocessed image
 */
async function preprocessForOCR(imageBuffer) {
  try {
    return await sharp(imageBuffer)
      .grayscale()
      .normalize()
      .sharpen()
      .threshold(150)
      .toBuffer();
  } catch (error) {
    console.error('OCR preprocessing error:', error.message);
    return imageBuffer;
  }
}

/**
 * Extract text from image
 * @param {string|Buffer} input - Image path or buffer
 * @param {Object} options - OCR options
 * @returns {Promise<Object>} - OCR result
 */
async function extractText(input, options = {}) {
  try {
    const sched = await initScheduler();
    
    let imageBuffer;
    if (Buffer.isBuffer(input)) {
      imageBuffer = input;
    } else if (typeof input === 'string' && fs.existsSync(input)) {
      imageBuffer = fs.readFileSync(input);
    } else {
      throw new Error('Invalid input for OCR');
    }

    // Preprocess if requested
    if (options.preprocess !== false) {
      imageBuffer = await preprocessForOCR(imageBuffer);
    }

    // Run OCR
    const result = await sched.addJob('recognize', imageBuffer);
    
    return {
      text: result.data.text.trim(),
      confidence: result.data.confidence,
      words: result.data.words?.map(w => ({
        text: w.text,
        confidence: w.confidence,
        bbox: w.bbox
      })) || []
    };
  } catch (error) {
    console.error('OCR extraction error:', error.message);
    return {
      text: '',
      confidence: 0,
      words: [],
      error: error.message
    };
  }
}

/**
 * Classify image type based on OCR content
 * @param {string} text - OCR text
 * @returns {string} - Image classification tag
 */
function classifyImageContent(text) {
  const lowerText = text.toLowerCase();
  
  // Nutrition facts indicators
  const nutritionPatterns = [
    'nutrition facts', 'nutritional information', 'per serving',
    'calories', 'total fat', 'sodium', 'carbohydrate', 'protein',
    'daily value', 'saturated fat', 'cholesterol', 'dietary fiber'
  ];
  
  // Ingredients indicators
  const ingredientPatterns = [
    'ingredients:', 'ingredients', 'contains:', 'may contain',
    'allergen', 'wheat', 'soy', 'milk', 'eggs', 'nuts'
  ];
  
  // Barcode area indicators
  const barcodePatterns = [
    /\d{8,14}/, // Long number sequences
    'ean', 'upc', 'gtin'
  ];
  
  // Front label indicators
  const frontPatterns = [
    'new', 'organic', 'natural', 'best before',
    'net weight', 'net wt', 'oz', 'ml', 'g '
  ];
  
  if (nutritionPatterns.some(p => lowerText.includes(p))) {
    return 'nutrition';
  }
  
  if (ingredientPatterns.some(p => lowerText.includes(p))) {
    return 'ingredients';
  }
  
  for (const pattern of barcodePatterns) {
    if (typeof pattern === 'string' && lowerText.includes(pattern)) {
      return 'barcode';
    }
    if (pattern instanceof RegExp && pattern.test(text)) {
      return 'barcode';
    }
  }
  
  if (frontPatterns.some(p => lowerText.includes(p))) {
    return 'front';
  }
  
  return 'other';
}

/**
 * Parse structured fields from OCR text
 * @param {string} text - OCR text
 * @returns {Object} - Parsed fields
 */
function parseOCRFields(text) {
  const fields = {};
  const lines = text.split('\n').map(l => l.trim()).filter(l => l);
  
  // Net weight patterns
  const weightPatterns = [
    /net\s*(?:weight|wt\.?)[:\s]*(\d+(?:\.\d+)?\s*(?:g|kg|oz|lb|ml|l))/i,
    /(\d+(?:\.\d+)?\s*(?:g|kg|oz|lb|ml|l))\s*net/i
  ];
  
  for (const pattern of weightPatterns) {
    const match = text.match(pattern);
    if (match) {
      fields.net_weight = match[1].trim();
      break;
    }
  }
  
  // Ingredients extraction
  const ingredientsMatch = text.match(/ingredients[:\s]*(.+?)(?=nutrition|contains|allergen|$)/is);
  if (ingredientsMatch) {
    fields.ingredients = ingredientsMatch[1].trim().replace(/\n/g, ' ');
  }
  
  // Origin country
  const originPatterns = [
    /(?:product of|made in|origin)[:\s]*([a-z\s]+?)(?:\.|,|$)/i,
    /(?:country of origin)[:\s]*([a-z\s]+?)(?:\.|,|$)/i
  ];
  
  for (const pattern of originPatterns) {
    const match = text.match(pattern);
    if (match) {
      fields.origin = match[1].trim();
      break;
    }
  }
  
  // Manufacturer
  const mfgPatterns = [
    /(?:manufactured by|distributed by|produced by)[:\s]*(.+?)(?:\.|,|$)/i,
    /(?:mfg by|dist by)[:\s]*(.+?)(?:\.|,|$)/i
  ];
  
  for (const pattern of mfgPatterns) {
    const match = text.match(pattern);
    if (match) {
      fields.manufacturer = match[1].trim();
      break;
    }
  }
  
  // Storage instructions
  const storagePatterns = [
    /(?:store|keep|storage)[:\s]*(.+?)(?:\.|$)/i,
    /(?:refrigerate|freeze|keep frozen)/i
  ];
  
  for (const pattern of storagePatterns) {
    const match = text.match(pattern);
    if (match) {
      fields.storage = match[0].trim();
      break;
    }
  }
  
  // Halal/Kosher certification
  if (/halal/i.test(text)) {
    fields.halal = true;
  }
  if (/kosher/i.test(text)) {
    fields.kosher = true;
  }
  
  // Expiry mentions
  const expiryPatterns = [
    /(?:best before|use by|expiry|exp)[:\s]*(.+?)(?:\.|$)/i,
    /(?:bb|exp)[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i
  ];
  
  for (const pattern of expiryPatterns) {
    const match = text.match(pattern);
    if (match) {
      fields.expiry = match[1].trim();
      break;
    }
  }
  
  // Extract potential barcode digits
  const barcodeMatch = text.match(/\b(\d{8}|\d{12}|\d{13}|\d{14})\b/);
  if (barcodeMatch) {
    fields.potential_barcode = barcodeMatch[1];
  }
  
  return fields;
}

/**
 * Process multiple images and merge OCR results
 * @param {Array<string|Buffer>} images - Array of images
 * @returns {Promise<Object>} - Merged OCR result
 */
async function processProductImages(images) {
  const results = {
    images: [],
    merged_text: '',
    parsed_fields: {},
    tags: []
  };
  
  for (let i = 0; i < images.length; i++) {
    const image = images[i];
    const ocrResult = await extractText(image);
    
    const tag = classifyImageContent(ocrResult.text);
    const fields = parseOCRFields(ocrResult.text);
    
    results.images.push({
      index: i,
      text: ocrResult.text,
      confidence: ocrResult.confidence,
      tag,
      fields
    });
    
    results.merged_text += ocrResult.text + '\n';
    results.tags.push(tag);
    
    // Merge fields
    for (const [key, value] of Object.entries(fields)) {
      if (!results.parsed_fields[key]) {
        results.parsed_fields[key] = value;
      }
    }
  }
  
  return results;
}

/**
 * Extract barcode digits from OCR as fallback
 * @param {string} text - OCR text
 * @returns {string|null} - Potential barcode
 */
function extractBarcodeFromOCR(text) {
  // Look for standard barcode patterns
  const patterns = [
    /\b(\d{13})\b/, // EAN-13
    /\b(\d{12})\b/, // UPC-A
    /\b(\d{8})\b/,  // EAN-8, UPC-E
    /\b(\d{14})\b/  // ITF-14
  ];
  
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      return match[1];
    }
  }
  
  return null;
}

module.exports = {
  initScheduler,
  terminateScheduler,
  extractText,
  classifyImageContent,
  parseOCRFields,
  processProductImages,
  extractBarcodeFromOCR,
  preprocessForOCR
};

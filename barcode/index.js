/**
 * Barcode Detection Module
 * Uses ZXing library for barcode decoding with image preprocessing
 */

const sharp = require('sharp');
const path = require('path');
const fs = require('fs');

// ZXing requires dynamic import (ESM)
let zxing = null;

async function initZXing() {
  if (!zxing) {
    try {
      // Try to use the zxing-wasm package
      const { readBarcodesFromImageData } = await import('zxing-wasm');
      zxing = { readBarcodesFromImageData };
    } catch (e) {
      console.log('ZXing-wasm not available, using fallback...');
      // Fallback: we'll use a simpler approach
      zxing = { readBarcodesFromImageData: null };
    }
  }
  return zxing;
}

/**
 * Preprocess image for better barcode detection
 * @param {Buffer} imageBuffer - Original image buffer
 * @returns {Promise<Buffer[]>} - Array of preprocessed image buffers
 */
async function preprocessImage(imageBuffer) {
  const variants = [];

  try {
    // Original grayscale
    const grayscale = await sharp(imageBuffer)
      .grayscale()
      .toBuffer();
    variants.push(grayscale);

    // High contrast
    const highContrast = await sharp(imageBuffer)
      .grayscale()
      .normalize()
      .sharpen()
      .toBuffer();
    variants.push(highContrast);

    // Inverted (for light barcodes on dark background)
    const inverted = await sharp(imageBuffer)
      .grayscale()
      .negate()
      .toBuffer();
    variants.push(inverted);

    // Resized larger (helps with small barcodes)
    const metadata = await sharp(imageBuffer).metadata();
    if (metadata.width < 1000) {
      const enlarged = await sharp(imageBuffer)
        .grayscale()
        .resize(1500, null, { withoutEnlargement: false })
        .sharpen()
        .toBuffer();
      variants.push(enlarged);
    }

    // Threshold (binary)
    const threshold = await sharp(imageBuffer)
      .grayscale()
      .threshold(128)
      .toBuffer();
    variants.push(threshold);

  } catch (error) {
    console.error('Error preprocessing image:', error.message);
    // Return at least the original
    variants.push(imageBuffer);
  }

  return variants;
}

/**
 * Decode barcode from image buffer using ZXing
 * @param {Buffer} imageBuffer - Image buffer
 * @returns {Promise<Object|null>} - Barcode result or null
 */
async function decodeWithZXing(imageBuffer) {
  try {
    const zx = await initZXing();
    
    if (!zx.readBarcodesFromImageData) {
      // Fallback: use pattern matching for common barcode formats
      return await fallbackBarcodeDetection(imageBuffer);
    }

    // Get image data
    const { data, info } = await sharp(imageBuffer)
      .raw()
      .ensureAlpha()
      .toBuffer({ resolveWithObject: true });

    const imageData = new ImageData(
      new Uint8ClampedArray(data),
      info.width,
      info.height
    );

    const results = await zx.readBarcodesFromImageData(imageData, {
      formats: ['EAN-13', 'EAN-8', 'UPC-A', 'UPC-E', 'Code128', 'Code39', 'QRCode'],
      tryHarder: true,
      maxNumberOfSymbols: 5
    });

    if (results && results.length > 0) {
      const best = results[0];
      return {
        value: best.text,
        format: best.format,
        confidence: 0.95
      };
    }

    return null;
  } catch (error) {
    console.error('ZXing decode error:', error.message);
    return null;
  }
}

/**
 * Fallback barcode detection using pattern matching
 * This is a simplified approach when ZXing is not available
 */
async function fallbackBarcodeDetection(imageBuffer) {
  // For now, return null - OCR will be used as backup
  return null;
}

/**
 * Main barcode detection function
 * Tries multiple preprocessing variants
 * @param {string|Buffer} input - Image path or buffer
 * @returns {Promise<Object|null>} - Best barcode result
 */
async function detectBarcode(input) {
  try {
    let imageBuffer;
    
    if (Buffer.isBuffer(input)) {
      imageBuffer = input;
    } else if (typeof input === 'string') {
      if (!fs.existsSync(input)) {
        console.error('Image file not found:', input);
        return null;
      }
      imageBuffer = fs.readFileSync(input);
    } else {
      console.error('Invalid input type for barcode detection');
      return null;
    }

    // Preprocess image into multiple variants
    const variants = await preprocessImage(imageBuffer);
    
    // Try to decode each variant
    for (const variant of variants) {
      const result = await decodeWithZXing(variant);
      if (result) {
        return result;
      }
    }

    return null;
  } catch (error) {
    console.error('Barcode detection error:', error.message);
    return null;
  }
}

/**
 * Batch process multiple images
 * @param {Array} images - Array of image paths or buffers
 * @returns {Promise<Array>} - Array of results
 */
async function detectBarcodesBatch(images) {
  const results = [];
  
  for (const image of images) {
    const result = await detectBarcode(image);
    results.push(result);
  }
  
  return results;
}

/**
 * Validate barcode format
 * @param {string} value - Barcode value
 * @param {string} format - Expected format
 * @returns {boolean} - Whether valid
 */
function validateBarcode(value, format) {
  if (!value) return false;
  
  const patterns = {
    'EAN-13': /^\d{13}$/,
    'EAN-8': /^\d{8}$/,
    'UPC-A': /^\d{12}$/,
    'UPC-E': /^\d{8}$/,
    'Code128': /^[\x00-\x7F]+$/,
    'Code39': /^[A-Z0-9\-\.\ \$\/\+\%]+$/
  };
  
  const pattern = patterns[format];
  if (!pattern) return true; // Unknown format, assume valid
  
  return pattern.test(value);
}

/**
 * Calculate EAN-13 check digit
 * @param {string} digits - First 12 digits
 * @returns {number} - Check digit
 */
function calculateEAN13CheckDigit(digits) {
  if (digits.length !== 12) return -1;
  
  let sum = 0;
  for (let i = 0; i < 12; i++) {
    const digit = parseInt(digits[i], 10);
    sum += i % 2 === 0 ? digit : digit * 3;
  }
  
  return (10 - (sum % 10)) % 10;
}

module.exports = {
  detectBarcode,
  detectBarcodesBatch,
  validateBarcode,
  calculateEAN13CheckDigit,
  preprocessImage
};

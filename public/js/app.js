/**
 * Supermarket Crawler Dashboard - JavaScript Application
 * Handles UI interactions and API communication
 */

// ==================== State ====================
const state = {
  activeJobId: null,
  pollingInterval: null,
  currentPage: 1,
  productsPerPage: 20
};

// ==================== DOM Elements ====================
const elements = {
  statusBar: document.getElementById('statusBar'),
  serverStatus: document.getElementById('serverStatus'),
  totalProducts: document.getElementById('totalProducts'),
  crawlForm: document.getElementById('crawlForm'),
  homepageUrl: document.getElementById('homepageUrl'),
  enableOCR: document.getElementById('enableOCR'),
  enableBarcode: document.getElementById('enableBarcode'),
  concurrency: document.getElementById('concurrency'),
  requestDelay: document.getElementById('requestDelay'),
  startBtn: document.getElementById('startBtn'),
  activeCrawl: document.getElementById('activeCrawl'),
  activeJobId: document.getElementById('activeJobId'),
  activeUrl: document.getElementById('activeUrl'),
  activeStatus: document.getElementById('activeStatus'),
  progressFill: document.getElementById('progressFill'),
  processedProducts: document.getElementById('processedProducts'),
  processedCategories: document.getElementById('processedCategories'),
  detectedBarcodes: document.getElementById('detectedBarcodes'),
  crawlErrors: document.getElementById('crawlErrors'),
  stopBtn: document.getElementById('stopBtn'),
  errorLog: document.getElementById('errorLog'),
  errorList: document.getElementById('errorList'),
  statsGrid: document.getElementById('statsGrid'),
  statTotalProducts: document.getElementById('statTotalProducts'),
  statTotalBarcodes: document.getElementById('statTotalBarcodes'),
  statTotalJobs: document.getElementById('statTotalJobs'),
  statCompletedJobs: document.getElementById('statCompletedJobs'),
  sitesList: document.getElementById('sitesList'),
  productsBody: document.getElementById('productsBody'),
  pagination: document.getElementById('pagination'),
  jobsBody: document.getElementById('jobsBody'),
  productModal: document.getElementById('productModal'),
  modalClose: document.getElementById('modalClose'),
  modalBody: document.getElementById('modalBody')
};

// ==================== API Functions ====================
const api = {
  async getStatus() {
    const res = await fetch('/api/status');
    return res.json();
  },

  async startCrawl(url, config) {
    const res = await fetch('/api/crawl/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, config })
    });
    return res.json();
  },

  async stopCrawl(jobId) {
    const res = await fetch(`/api/crawl/stop/${jobId}`, { method: 'POST' });
    return res.json();
  },

  async getCrawlStatus(jobId) {
    const res = await fetch(`/api/crawl/status/${jobId}`);
    return res.json();
  },

  async getJobs() {
    const res = await fetch('/api/crawl/jobs');
    return res.json();
  },

  async getProducts(page = 1, limit = 20) {
    const res = await fetch(`/api/products?page=${page}&limit=${limit}`);
    return res.json();
  },

  async getProduct(productId) {
    const res = await fetch(`/api/products/${productId}`);
    return res.json();
  },

  async getStats() {
    const res = await fetch('/api/stats');
    return res.json();
  },

  async getBarcodes() {
    const res = await fetch('/api/barcodes');
    return res.json();
  }
};

// ==================== UI Functions ====================
function updateServerStatus(status) {
  elements.serverStatus.textContent = status.status === 'running' ? 'Connected' : 'Disconnected';
  elements.statusBar.classList.toggle('connected', status.status === 'running');
  elements.statusBar.classList.toggle('error', status.status !== 'running');
  elements.totalProducts.textContent = status.totalProducts || 0;
}

function showActiveCrawl(job) {
  state.activeJobId = job.job_id;
  elements.activeCrawl.style.display = 'block';
  elements.activeJobId.textContent = job.job_id.substring(0, 8) + '...';
  elements.activeUrl.textContent = job.homepage_url;
  updateCrawlStatus(job);
  
  // Start polling
  if (!state.pollingInterval) {
    state.pollingInterval = setInterval(pollCrawlStatus, 2000);
  }
}

function updateCrawlStatus(job) {
  // Update status badge
  elements.activeStatus.textContent = job.status;
  elements.activeStatus.className = `status-badge ${job.status}`;
  
  // Update stats
  const processed = job.processed_products || (job.liveStats?.products) || 0;
  const total = job.total_products || processed + 10;
  const progress = total > 0 ? Math.min((processed / total) * 100, 100) : 0;
  
  elements.progressFill.style.width = `${progress}%`;
  elements.processedProducts.textContent = processed;
  elements.processedCategories.textContent = job.processed_categories || (job.liveStats?.categories) || 0;
  elements.detectedBarcodes.textContent = job.liveStats?.barcodes || 0;
  elements.crawlErrors.textContent = job.errors || (job.liveStats?.errors) || 0;
  
  // Update errors
  if (job.errorLog && job.errorLog.length > 0) {
    elements.errorLog.style.display = 'block';
    elements.errorList.innerHTML = job.errorLog
      .slice(-10)
      .map(e => `<li>${e.timestamp}: ${e.error}</li>`)
      .join('');
  } else {
    elements.errorLog.style.display = 'none';
  }
  
  // Check if completed
  if (['completed', 'failed', 'stopped'].includes(job.status)) {
    stopPolling();
    elements.startBtn.disabled = false;
    refreshData();
  }
}

function hideActiveCrawl() {
  elements.activeCrawl.style.display = 'none';
  state.activeJobId = null;
  stopPolling();
}

async function pollCrawlStatus() {
  if (!state.activeJobId) return;
  
  try {
    const job = await api.getCrawlStatus(state.activeJobId);
    updateCrawlStatus(job);
  } catch (error) {
    console.error('Polling error:', error);
  }
}

function stopPolling() {
  if (state.pollingInterval) {
    clearInterval(state.pollingInterval);
    state.pollingInterval = null;
  }
}

function renderProducts(data) {
  if (!data.products || data.products.length === 0) {
    elements.productsBody.innerHTML = `
      <tr>
        <td colspan="7" class="empty-state">No products yet. Start a crawl to populate the database.</td>
      </tr>
    `;
    return;
  }
  
  elements.productsBody.innerHTML = data.products.map(p => `
    <tr>
      <td><code>${p.product_id}</code></td>
      <td>${escapeHtml(p.name || '-')}</td>
      <td>${escapeHtml(p.brand || '-')}</td>
      <td>${escapeHtml(truncate(p.category, 30) || '-')}</td>
      <td>${p.price ? `${p.currency || ''}${p.price}` : '-'}</td>
      <td>${p.barcodes ? `<code>${p.barcodes.split(',')[0]}</code>` : '-'}</td>
      <td>
        <button class="btn btn-secondary view-btn" onclick="viewProduct('${p.product_id}')">View</button>
      </td>
    </tr>
  `).join('');
  
  renderPagination(data.total, data.page, data.limit);
}

function renderPagination(total, currentPage, limit) {
  const totalPages = Math.ceil(total / limit);
  if (totalPages <= 1) {
    elements.pagination.innerHTML = '';
    return;
  }
  
  let html = '';
  
  if (currentPage > 1) {
    html += `<button onclick="loadProducts(${currentPage - 1})">← Prev</button>`;
  }
  
  for (let i = 1; i <= Math.min(totalPages, 5); i++) {
    html += `<button class="${i === currentPage ? 'active' : ''}" onclick="loadProducts(${i})">${i}</button>`;
  }
  
  if (currentPage < totalPages) {
    html += `<button onclick="loadProducts(${currentPage + 1})">Next →</button>`;
  }
  
  elements.pagination.innerHTML = html;
}

function renderJobs(jobs) {
  if (!jobs || jobs.length === 0) {
    elements.jobsBody.innerHTML = `
      <tr>
        <td colspan="6" class="empty-state">No crawl history yet.</td>
      </tr>
    `;
    return;
  }
  
  elements.jobsBody.innerHTML = jobs.map(job => {
    const started = job.started_at ? new Date(job.started_at) : null;
    const finished = job.finished_at ? new Date(job.finished_at) : null;
    const duration = started && finished 
      ? formatDuration(finished - started)
      : (started ? 'In progress' : '-');
    
    return `
      <tr>
        <td><code>${job.job_id.substring(0, 8)}...</code></td>
        <td>${escapeHtml(truncate(job.homepage_url, 40))}</td>
        <td><span class="status-badge ${job.status}">${job.status}</span></td>
        <td>${job.processed_products || 0}</td>
        <td>${started ? started.toLocaleString() : '-'}</td>
        <td>${duration}</td>
      </tr>
    `;
  }).join('');
}

function renderStats(stats) {
  elements.statTotalProducts.textContent = stats.totalProducts || 0;
  elements.statTotalBarcodes.textContent = stats.uniqueBarcodes || 0;
  elements.statTotalJobs.textContent = stats.totalJobs || 0;
  elements.statCompletedJobs.textContent = stats.completedJobs || 0;
  
  // Render sites breakdown
  if (stats.productsBySite) {
    elements.sitesList.innerHTML = Object.entries(stats.productsBySite)
      .map(([site, count]) => `
        <span class="site-tag">
          ${escapeHtml(new URL(site).hostname)}
          <span class="count">${count}</span>
        </span>
      `)
      .join('');
  }
}

async function viewProduct(productId) {
  try {
    const product = await api.getProduct(productId);
    
    elements.modalBody.innerHTML = `
      <div class="product-detail">
        <h3>${escapeHtml(product.name || 'Unknown Product')}</h3>
        
        <div class="meta">
          <div class="meta-item">
            <strong>Product ID</strong>
            <code>${product.product_id}</code>
          </div>
          <div class="meta-item">
            <strong>Brand</strong>
            ${escapeHtml(product.brand || '-')}
          </div>
          <div class="meta-item">
            <strong>Category</strong>
            ${escapeHtml(product.category || '-')}
          </div>
          <div class="meta-item">
            <strong>Price</strong>
            ${product.price ? `${product.currency || ''}${product.price}` : '-'}
          </div>
          <div class="meta-item">
            <strong>Variant</strong>
            ${escapeHtml(product.variant || '-')}
          </div>
          <div class="meta-item">
            <strong>Availability</strong>
            ${escapeHtml(product.availability || '-')}
          </div>
          <div class="meta-item">
            <strong>Manufacturer</strong>
            ${escapeHtml(product.manufacturer || '-')}
          </div>
          <div class="meta-item">
            <strong>Origin</strong>
            ${escapeHtml(product.origin || '-')}
          </div>
        </div>
        
        ${product.description ? `
          <div class="meta-item" style="margin-bottom: 16px;">
            <strong>Description</strong>
            <p>${escapeHtml(product.description)}</p>
          </div>
        ` : ''}
        
        ${product.ingredients ? `
          <div class="meta-item" style="margin-bottom: 16px;">
            <strong>Ingredients</strong>
            <p>${escapeHtml(product.ingredients)}</p>
          </div>
        ` : ''}
        
        ${product.images && product.images.length > 0 ? `
          <h4>Images (${product.images.length})</h4>
          <div class="product-images">
            ${product.images.map(img => `
              <div>
                <img src="/images/${product.product_id}/${img.local_path.split('/').pop()}" 
                     alt="${img.tag}" 
                     onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>📷</text></svg>'">
                <div class="image-tag">
                  ${img.tag}
                  ${img.barcode_value ? `<br><code>${img.barcode_value}</code>` : ''}
                </div>
              </div>
            `).join('')}
          </div>
        ` : ''}
        
        <div style="margin-top: 20px;">
          <a href="${escapeHtml(product.url)}" target="_blank" class="btn btn-secondary">
            View Original Page ↗
          </a>
        </div>
      </div>
    `;
    
    elements.productModal.classList.add('show');
  } catch (error) {
    console.error('Error loading product:', error);
    alert('Failed to load product details');
  }
}

// ==================== Helper Functions ====================
function escapeHtml(text) {
  if (!text) return '';
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function truncate(text, maxLength) {
  if (!text) return '';
  return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
}

function formatDuration(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${seconds}s`;
}

// ==================== Data Loading ====================
async function loadProducts(page = 1) {
  try {
    state.currentPage = page;
    const data = await api.getProducts(page, state.productsPerPage);
    renderProducts(data);
  } catch (error) {
    console.error('Error loading products:', error);
  }
}

async function refreshData() {
  try {
    const [status, stats, jobs] = await Promise.all([
      api.getStatus(),
      api.getStats(),
      api.getJobs()
    ]);
    
    updateServerStatus(status);
    renderStats(stats);
    renderJobs(jobs);
    await loadProducts(state.currentPage);
    
    // Check for any running jobs
    const runningJob = jobs.find(j => j.status === 'running');
    if (runningJob && !state.activeJobId) {
      showActiveCrawl(runningJob);
    }
  } catch (error) {
    console.error('Error refreshing data:', error);
    elements.statusBar.classList.add('error');
    elements.serverStatus.textContent = 'Connection Error';
  }
}

// ==================== Event Handlers ====================
elements.crawlForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  
  const url = elements.homepageUrl.value.trim();
  if (!url) {
    alert('Please enter a valid URL');
    return;
  }
  
  const config = {
    enableOCR: elements.enableOCR.checked,
    enableBarcode: elements.enableBarcode.checked,
    concurrency: parseInt(elements.concurrency.value) || 2,
    requestDelay: parseInt(elements.requestDelay.value) || 1000
  };
  
  elements.startBtn.disabled = true;
  elements.startBtn.textContent = 'Starting...';
  
  try {
    const result = await api.startCrawl(url, config);
    
    if (result.success) {
      showActiveCrawl({
        job_id: result.jobId,
        homepage_url: url,
        status: 'running'
      });
      elements.homepageUrl.value = '';
    } else {
      alert('Failed to start crawl: ' + (result.error || 'Unknown error'));
      elements.startBtn.disabled = false;
    }
  } catch (error) {
    console.error('Error starting crawl:', error);
    alert('Failed to start crawl: ' + error.message);
    elements.startBtn.disabled = false;
  }
  
  elements.startBtn.textContent = '▶ Start Crawl';
});

elements.stopBtn.addEventListener('click', async () => {
  if (!state.activeJobId) return;
  
  elements.stopBtn.disabled = true;
  elements.stopBtn.textContent = 'Stopping...';
  
  try {
    await api.stopCrawl(state.activeJobId);
  } catch (error) {
    console.error('Error stopping crawl:', error);
  }
  
  elements.stopBtn.disabled = false;
  elements.stopBtn.textContent = '⏹ Stop Crawl';
});

elements.modalClose.addEventListener('click', () => {
  elements.productModal.classList.remove('show');
});

elements.productModal.addEventListener('click', (e) => {
  if (e.target === elements.productModal) {
    elements.productModal.classList.remove('show');
  }
});

// Make viewProduct available globally
window.viewProduct = viewProduct;
window.loadProducts = loadProducts;

// ==================== Initialize ====================
document.addEventListener('DOMContentLoaded', () => {
  refreshData();
  
  // Refresh data periodically
  setInterval(refreshData, 30000);
});

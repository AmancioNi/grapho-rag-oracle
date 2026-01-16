// CineGen AI - JavaScript Application

const API_BASE_URL = 'http://localhost:8000/api';

let currentCustomerId = null;
let currentWatchMovieId = null;
let catalogOffset = 0;
const catalogLimit = 20;
let catalogHasMore = true;
let catalogCurrentSearch = '';
let Graph3D = null;

// ===== INITIALIZATION =====
window.onload = () => {
  lucide.createIcons();
  checkAPIHealth();
  loadCatalog(true, '');
  loadCustomers();
};

// ===== API HEALTH =====
async function checkAPIHealth() {
  const statusEl = document.getElementById('sidebar-status');
  try {
    const response = await fetch(`${API_BASE_URL}/health`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /health HTTP Error:', response.status, raw);
      statusEl.textContent = 'Offline';
      statusEl.className = 'text-red-500';
      return;
    }
    const data = await response.json();
    if (data.status === 'healthy') {
      statusEl.textContent = 'v2.5.0 Online';
      statusEl.className = 'text-emerald-500';
    } else {
      statusEl.textContent = 'Offline';
      statusEl.className = 'text-red-500';
    }
  } catch (error) {
    statusEl.textContent = 'Offline';
    statusEl.className = 'text-red-500';
  }
}

// ===== TAB SWITCHING =====
function switchTab(tabId) {
  document.querySelectorAll('.view-section').forEach(el => el.classList.add('hidden'));
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.remove('bg-red-600/10', 'text-red-500', 'border', 'border-red-900/50');
    el.classList.add('text-zinc-400');
  });

  const view = document.getElementById(`view-${tabId}`);
  if (view) view.classList.remove('hidden');

  const activeBtn = document.getElementById(`btn-${tabId}`);
  if (activeBtn) {
    activeBtn.classList.remove('text-zinc-400');
    activeBtn.classList.add('bg-red-600/10', 'text-red-500', 'border', 'border-red-900/50');
  }

  if (tabId === 'watched') loadWatchedMovies();
  if (tabId === 'graph') loadCustomersForGraph();
  if (tabId === 'compare') loadCustomersForCompare();
  if (tabId === 'network3d') loadCustomersForNetwork3D();

  if (tabId !== 'network3d' && Graph3D) {
    Graph3D = null;
    const c = document.getElementById('graph-3d-container');
    if (c) c.innerHTML = '';
  }

  lucide.createIcons();
}

// ===== CATALOG =====
async function loadCatalog(reset = true, searchQuery = '') {
  const grid = document.getElementById('catalog-grid');
  const btn = document.getElementById('catalog-loadmore');
  const clearBtn = document.getElementById('catalog-clear-search');

  if (reset) {
    catalogOffset = 0;
    catalogHasMore = true;
    catalogCurrentSearch = searchQuery;
    grid.innerHTML = '<div class="col-span-full text-center py-20"><div class="animate-spin w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full mx-auto mb-4"></div></div>';
    if (btn) btn.classList.add('hidden');
  }

  if (clearBtn) {
    if (catalogCurrentSearch) clearBtn.classList.remove('hidden');
    else clearBtn.classList.add('hidden');
  }

  try {
    let url = `${API_BASE_URL}/movies?limit=${catalogLimit}&offset=${catalogOffset}`;
    if (catalogCurrentSearch) {
      url += `&search=${encodeURIComponent(catalogCurrentSearch)}`;
    }

    const response = await fetch(url);

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /movies HTTP Error:', response.status, raw);
      grid.innerHTML = `
        <div class="col-span-full text-center py-16 px-6">
          <div class="text-red-500 font-bold mb-2">Erro ${response.status} ao carregar filmes</div>
          <div class="text-xs text-zinc-400 max-w-3xl mx-auto whitespace-pre-wrap break-words">${raw}</div>
        </div>
      `;
      return;
    }

    const data = await response.json();

    if (data.success) {
      if (reset) grid.innerHTML = '';
      
      if (!data.data || data.data.length === 0) {
        if (reset) {
          grid.innerHTML = `
            <div class="col-span-full text-center py-20 text-zinc-500">
              <i data-lucide="search-x" class="w-16 h-16 mx-auto mb-4 text-zinc-600"></i>
              <p class="text-lg font-medium mb-2">Nenhum filme encontrado</p>
              <p class="text-sm">Tente buscar por outro termo</p>
            </div>
          `;
          lucide.createIcons();
        }
        catalogHasMore = false;
        if (btn) btn.classList.add('hidden');
        return;
      }

      renderMoviesAppend(data.data || []);

      if (!data.data || data.data.length < catalogLimit) {
        catalogHasMore = false;
        if (btn) btn.classList.add('hidden');
      } else {
        catalogHasMore = true;
        if (btn) btn.classList.remove('hidden');
      }

      catalogOffset += (data.data?.length || 0);
      
      document.getElementById('movie-count').textContent = data.total || 0;
      
      if (catalogCurrentSearch && reset) {
        grid.insertAdjacentHTML('afterbegin', `
          <div class="col-span-full mb-4">
            <div class="bg-zinc-900 border border-zinc-700 rounded-lg p-4 flex items-center gap-3">
              <i data-lucide="info" class="text-blue-500"></i>
              <p class="text-sm text-zinc-300">
                <strong>${data.total}</strong> resultado(s) encontrado(s) para 
                <strong class="text-red-500">"${catalogCurrentSearch}"</strong>
              </p>
            </div>
          </div>
        `);
        lucide.createIcons();
      }
    } else {
      console.error('❌ /movies API error payload:', data);
      grid.innerHTML = `
        <div class="col-span-full text-center py-16 text-red-500">
          Erro ao carregar filmes<br/>
          <span class="text-xs text-zinc-400">${data.error || 'Sem detalhes'}</span>
        </div>
      `;
    }
  } catch (error) {
    console.error('❌ /movies fetch exception:', error);
    grid.innerHTML = `
      <div class="col-span-full text-center py-16 text-red-500">
        Erro ao conectar com API<br/>
        <span class="text-xs text-zinc-400">${error?.message || error}</span>
      </div>
    `;
  }
}

function loadMoreCatalog() {
  if (!catalogHasMore) return;
  loadCatalog(false, catalogCurrentSearch);
}

let catalogSearchTimeout;
function handleCatalogSearch(event) {
  const searchValue = event.target.value.trim();
  clearTimeout(catalogSearchTimeout);
  catalogSearchTimeout = setTimeout(() => {
    loadCatalog(true, searchValue);
  }, 500);
}

function clearCatalogSearch() {
  const input = document.getElementById('catalog-search-input');
  if (input) {
    input.value = '';
    loadCatalog(true, '');
  }
}

function renderMoviesAppend(movies) {
  const grid = document.getElementById('catalog-grid');
  
  movies.forEach(movie => {
    const genres = typeof movie.genres === 'object' ? Object.keys(movie.genres).slice(0, 2).join(', ') : 'N/A';
    
    const card = document.createElement('div');
    card.className = 'bg-zinc-950 border border-zinc-800 rounded-2xl overflow-hidden group movie-card transition-all';
    card.innerHTML = `
      <div class="aspect-[2/3] bg-zinc-800 relative overflow-hidden">
        <img src="${movie.poster_url || 'https://via.placeholder.com/300x450?text=' + encodeURIComponent(movie.title)}"
             class="w-full h-full object-cover opacity-90 group-hover:opacity-100 transition-opacity"
             onerror="this.src='https://via.placeholder.com/300x450?text=No+Image'">
        <div class="absolute inset-0 bg-gradient-to-t from-black/80 to-transparent"></div>
        <div class="absolute bottom-4 left-4">
          <span class="bg-red-600 text-[10px] font-bold px-2 py-0.5 rounded uppercase">${genres}</span>
        </div>
        <div class="absolute top-3 right-3 bg-black/60 backdrop-blur-md px-2 py-1 rounded text-xs font-bold text-yellow-400">
          ★ ${(movie.rating || 8.0).toFixed(1)}
        </div>
        ${movie.trailer_url ? `
          <button onclick="openTrailerModal('${movie.title.replace(/'/g, "\\'")}', '${movie.trailer_url}')"
            class="absolute bottom-4 right-4 bg-red-600 hover:bg-red-700 text-white px-3 py-1.5 rounded-lg text-xs font-bold transition-all flex items-center gap-1 shadow-lg opacity-0 group-hover:opacity-100">
            <i data-lucide="play" size="12"></i> Trailer
          </button>
        ` : ''}
      </div>
      <div class="p-4">
        <h3 class="font-bold text-white text-sm mb-1 truncate">${movie.title}</h3>
        <p class="text-xs text-zinc-500 mb-3">${movie.year || '2024'}</p>
        <button onclick="openWatchModal(${movie.id})" 
          class="w-full py-2 bg-zinc-900 hover:bg-zinc-800 border border-zinc-800 rounded-lg text-xs transition-colors font-medium">
          Marcar como Assistido
        </button>
      </div>
    `;
    grid.appendChild(card);
  });
  lucide.createIcons();
}

// ===== TRAILER MODAL =====
function openTrailerModal(title, trailerUrl) {
  const modal = document.getElementById('trailer-modal');
  const iframe = document.getElementById('trailer-iframe');
  const titleEl = document.getElementById('trailer-title');

  let videoId = '';
  try {
    const url = new URL(trailerUrl);
    if (url.hostname.includes('youtube.com')) {
      videoId = url.searchParams.get('v');
    } else if (url.hostname.includes('youtu.be')) {
      videoId = url.pathname.substring(1);
    }
  } catch (e) {
    console.error('Invalid URL:', e);
    showToast('⚠ URL de trailer inválida', 'error');
    return;
  }

  if (!videoId) {
    showToast('⚠ Não foi possível extrair ID do vídeo', 'error');
    return;
  }

  titleEl.textContent = title;
  iframe.src = `https://www.youtube.com/embed/${videoId}?autoplay=1`;
  modal.classList.remove('hidden');
  lucide.createIcons();
}

function closeTrailerModal(event) {
  if (event && event.target !== event.currentTarget && event.type === 'click') return;
  const modal = document.getElementById('trailer-modal');
  const iframe = document.getElementById('trailer-iframe');
  iframe.src = '';
  modal.classList.add('hidden');
}

// ===== VECTOR SEARCH =====
async function handleVectorSearch(e) {
  e.preventDefault();
  const query = document.getElementById('vector-input').value;
  const container = document.getElementById('vector-results');
  
  if (!query) return;

  container.innerHTML = `
    <div class="col-span-full text-center py-10">
      <div class="animate-spin w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full mx-auto mb-4"></div>
      <p class="text-red-500 font-mono text-xs">CALCULANDO SIMILARIDADE VETORIAL...</p>
    </div>
  `;

  try {
    const response = await fetch(`${API_BASE_URL}/search/vector`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: query, top_k: 10 })
    });

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /search/vector HTTP Error:', response.status, raw);
      container.innerHTML = `<div class="col-span-full text-red-500 text-center py-10">Erro ${response.status} na busca</div>`;
      return;
    }

    const data = await response.json();
    if (data.success) {
      renderVectorResults(data.results || []);
    } else {
      container.innerHTML = `<div class="col-span-full text-red-500 text-center py-10">Erro: ${data.error || 'Sem detalhes'}</div>`;
    }
  } catch (error) {
    container.innerHTML = '<div class="col-span-full text-red-500 text-center py-10">Erro na busca</div>';
  }
}

function renderVectorResults(results) {
  const container = document.getElementById('vector-results');
  
  if (!results || results.length === 0) {
    container.innerHTML = '<div class="col-span-full text-center py-20 text-zinc-500">Nenhum resultado encontrado</div>';
    return;
  }

  container.innerHTML = '';
  results.forEach((item, idx) => {
    const genres = typeof item.genres === 'object' ? Object.keys(item.genres).slice(0, 2).join(', ') : 'N/A';
    
    const card = document.createElement('div');
    card.className = 'bg-zinc-950 border border-zinc-800 rounded-2xl overflow-hidden hover:border-zinc-700 transition-all';
    card.innerHTML = `
      <div class="flex gap-4 p-4">
        <div class="w-24 h-36 flex-shrink-0">
          <img src="${item.poster_url || 'https://via.placeholder.com/200x300?text=' + encodeURIComponent(item.title)}"
               class="w-full h-full object-cover rounded-lg"
               onerror="this.src='https://via.placeholder.com/200x300?text=No+Image'">
        </div>
        <div class="flex-1 min-w-0">
          <div class="flex items-start justify-between mb-2">
            <div class="flex items-center gap-2">
              <span class="text-lg font-bold text-zinc-600">${idx + 1}</span>
              <h4 class="text-base font-bold text-white truncate">${item.title}</h4>
            </div>
            <span class="text-[10px] bg-red-600/20 text-red-400 px-2 py-1 rounded-full border border-red-900/50 font-mono flex-shrink-0">
              ${((item.score || 0) * 100).toFixed(1)}%
            </span>
          </div>
          <p class="text-xs text-zinc-400 italic leading-relaxed line-clamp-3 mb-2">"${item.snippet || item.summary || ''}"</p>
          <div class="text-[9px] text-zinc-500">#${genres}</div>
        </div>
      </div>
    `;
    container.appendChild(card);
  });
}

// ===== CUSTOMERS =====
async function loadCustomers() {
  try {
    const response = await fetch(`${API_BASE_URL}/customers`);

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers HTTP Error:', response.status, raw);
      const list = document.getElementById('customers-list');
      if (list) list.innerHTML = `<div class="col-span-full text-center py-10 text-red-500">Erro ${response.status} ao carregar clientes</div>`;
      return;
    }

    const data = await response.json();
    if (data.success) {
      renderCustomersList(data.data || []);
      updateCustomerSelect(data.data || []);
    }
  } catch (error) {
    console.error('Erro:', error);
  }
}

function updateCustomerSelect(customers) {
  const select = document.getElementById('customer-select');
  select.innerHTML = '<option value="">Escolha um cliente...</option>';
  
  customers.forEach(c => {
    const label = `${c.firstname} ${c.lastname}${c.email ? ' • ' + c.email : ''}`;
    select.innerHTML += `<option value="${c.id}">${label}</option>`;
  });
}

function renderCustomersList(customers) {
  const list = document.getElementById('customers-list');

  if (!customers || customers.length === 0) {
    list.innerHTML = '<div class="col-span-full text-center py-10 text-zinc-500">Nenhum cliente cadastrado</div>';
    return;
  }

  list.innerHTML = '';
  customers.forEach(customer => {
    list.innerHTML += `
      <div class="bg-zinc-950 border border-zinc-800 rounded-xl p-4 hover:border-zinc-700 transition-all">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-full bg-gradient-to-br from-indigo-600 to-purple-600 flex items-center justify-center text-white font-bold text-sm">
            ${customer.firstname.charAt(0)}${customer.lastname.charAt(0)}
          </div>
          <div class="flex-1">
            <p class="text-white font-medium text-sm">${customer.firstname} ${customer.lastname}</p>
            <p class="text-xs text-zinc-500">${customer.email || 'sem email'}</p>
            <p class="text-xs text-zinc-500 mt-1">${customer.movies_count || 0} filmes assistidos</p>
          </div>
          <button onclick="loadCustomerGraph(${customer.id}); switchTab('graph')"
            class="text-red-500 hover:text-red-400 text-xs transition-colors">
            <i data-lucide="share-2" size="14"></i>
          </button>
        </div>
      </div>
    `;
  });
  lucide.createIcons();
}

async function handleCreateCustomer(e) {
  e.preventDefault();

  const firstname = document.getElementById('customer-firstname').value.trim();
  const lastname = document.getElementById('customer-lastname').value.trim();
  const email = document.getElementById('customer-email').value.trim();
  
  if (!firstname || !lastname || !email) return;

  try {
    const response = await fetch(`${API_BASE_URL}/customers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ firstname, lastname, email })
    });

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers POST HTTP Error:', response.status, raw);
      showToast(`✗ Erro ${response.status} ao cadastrar`, 'error');
      return;
    }

    const data = await response.json();

    if (data.success) {
      document.getElementById('customer-firstname').value = '';
      document.getElementById('customer-lastname').value = '';
      document.getElementById('customer-email').value = '';
      loadCustomers();
      showToast('✓ Cliente cadastrado');
    } else {
      showToast('✗ Erro: ' + (data.error || 'Sem detalhes'), 'error');
    }
  } catch (error) {
    showToast('✗ Erro ao cadastrar', 'error');
  }
}

function selectCustomer() {
  const select = document.getElementById('customer-select');
  currentCustomerId = select.value ? parseInt(select.value) : null;
  
  const info = document.getElementById('selected-info');
  const nameEl = document.getElementById('selected-name');
  
  if (currentCustomerId) {
    const selectedText = select.options[select.selectedIndex].text;
    nameEl.textContent = selectedText;
    info.classList.remove('hidden');
    showToast('✓ Cliente: ' + selectedText);
  } else {
    info.classList.add('hidden');
  }
  
  lucide.createIcons();
}

// ===== WATCH MODAL =====
async function openWatchModal(movieId) {
  currentWatchMovieId = movieId;

  try {
    const response = await fetch(`${API_BASE_URL}/customers`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers (watch modal) HTTP Error:', response.status, raw);
      showToast('⚠ Erro ao carregar clientes', 'error');
      return;
    }

    const data = await response.json();

    if (data.success) {
      const list = document.getElementById('watch-customers-list');
      list.innerHTML = '';

      if (!data.data || data.data.length === 0) {
        list.innerHTML = '<div class="text-center py-4 text-zinc-500 text-sm">Nenhum cliente cadastrado</div>';
      } else {
        data.data.forEach(customer => {
          list.innerHTML += `
            <button onclick="markAsWatched(${customer.id})"
              class="w-full bg-zinc-800 hover:bg-zinc-700 border border-zinc-800 text-white px-4 py-3 rounded-xl transition-all text-left flex items-center gap-3">
              <div class="w-8 h-8 rounded-full bg-indigo-600 flex items-center justify-center text-white text-xs font-bold">
                ${customer.firstname.charAt(0)}${customer.lastname.charAt(0)}
              </div>
              <div class="min-w-0">
                <div class="text-sm truncate">${customer.firstname} ${customer.lastname}</div>
                <div class="text-[10px] text-zinc-400 truncate">${customer.email || ''}</div>
              </div>
            </button>
          `;
        });
      }

      document.getElementById('watch-modal').classList.remove('hidden');
      lucide.createIcons();
    }
  } catch (error) {
    showToast('⚠ Erro ao carregar clientes', 'error');
  }
}

function closeWatchModal(event) {
  if (event && event.target !== event.currentTarget) return;
  document.getElementById('watch-modal').classList.add('hidden');
  currentWatchMovieId = null;
}

async function markAsWatched(customerId) {
  if (!currentWatchMovieId) return;

  try {
    const response = await fetch(`${API_BASE_URL}/customers/${customerId}/watch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ movie_id: currentWatchMovieId })
    });

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /watch HTTP Error:', response.status, raw);
      showToast(`✗ Erro ${response.status}`, 'error');
      return;
    }

    const data = await response.json();

    if (data.success) {
      closeWatchModal();
      loadCustomers();
      showToast('✓ Sincronizado com o Property Graph');
    } else {
      showToast('✗ ' + (data.error || 'Falha'), 'error');
    }
  } catch (error) {
    showToast('✗ Erro de conexão', 'error');
  }
}

// ===== WATCHED MOVIES =====
async function loadWatchedMovies() {
  const container = document.getElementById('watched-grid');
  
  if (!currentCustomerId) {
    container.innerHTML = `
      <div class="col-span-full text-center py-20 text-zinc-500">
        <i data-lucide="user-x" class="w-16 h-16 mx-auto mb-4 opacity-20"></i>
        <p>Selecione um cliente para ver o histórico</p>
      </div>
    `;
    lucide.createIcons();
    return;
  }

  container.innerHTML = `
    <div class="col-span-full text-center py-10">
      <div class="animate-spin w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full mx-auto"></div>
    </div>
  `;

  try {
    const response = await fetch(`${API_BASE_URL}/customers/${currentCustomerId}/movies`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers/:id/movies HTTP Error:', response.status, raw);
      container.innerHTML = '<div class="col-span-full text-center text-red-500">Erro ao carregar</div>';
      return;
    }

    const data = await response.json();

    if (data.success && data.movies && data.movies.length > 0) {
      renderWatchedMovies(data.movies);
    } else {
      container.innerHTML = `
        <div class="col-span-full text-center py-20 text-zinc-500">
          <i data-lucide="film" class="w-16 h-16 mx-auto mb-4 opacity-20"></i>
          <p>Nenhum filme assistido ainda</p>
        </div>
      `;
    }
    lucide.createIcons();
  } catch (error) {
    container.innerHTML = '<div class="col-span-full text-center text-red-500">Erro ao carregar</div>';
  }
}

function renderWatchedMovies(movies) {
  const container = document.getElementById('watched-grid');
  container.innerHTML = '';

  movies.forEach(movie => {
    const card = document.createElement('div');
    card.className = 'bg-zinc-950 border border-zinc-800 rounded-2xl overflow-hidden';
    card.innerHTML = `
      <div class="h-44 bg-zinc-800 relative overflow-hidden">
        <img src="${movie.poster_url || 'https://via.placeholder.com/300x450?text=' + encodeURIComponent(movie.title)}"
             class="w-full h-full object-cover"
             onerror="this.src='https://via.placeholder.com/300x450?text=No+Image'">
        <div class="absolute top-3 right-3 bg-emerald-600 px-2 py-1 rounded text-xs font-bold">
          <i data-lucide="check" class="w-3 h-3 inline"></i> Visto
        </div>
      </div>
      <div class="p-4">
        <h4 class="font-bold text-white text-sm mb-1 truncate">${movie.title}</h4>
        <p class="text-xs text-zinc-500 line-clamp-2">${movie.summary || ''}</p>
      </div>
    `;
    container.appendChild(card);
  });
  lucide.createIcons();
}

// ===== GRAPH 2D =====
async function loadCustomersForGraph() {
  try {
    const response = await fetch(`${API_BASE_URL}/customers`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers (graph) HTTP Error:', response.status, raw);
      return;
    }
    const data = await response.json();

    if (data.success) {
      const container = document.getElementById('graph-customers-buttons');
      container.innerHTML = '';

      data.data.forEach(customer => {
        container.innerHTML += `
          <button onclick="loadCustomerGraph(${customer.id})"
            class="bg-zinc-900 hover:bg-zinc-800 px-6 py-2 rounded-full border border-zinc-800 text-white transition-all flex items-center gap-2 text-sm">
            <i data-lucide="user" size="14"></i> ${customer.firstname} ${customer.lastname}
          </button>
        `;
      });
      lucide.createIcons();
    }
  } catch (error) {
    console.error('Erro:', error);
  }
}

async function loadCustomerGraph(customerId, limit = 20) {
  const display = document.getElementById('graph-display');
  display.innerHTML = '<div class="text-center py-10"><div class="animate-spin w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full mx-auto mb-4"></div></div>';

  try {
    const response = await fetch(`${API_BASE_URL}/graph/customer/${customerId}?limit=${limit}`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /graph/customer HTTP Error:', response.status, raw);
      display.innerHTML = `<div class="text-red-500 text-center">Erro ${response.status}</div>`;
      return;
    }

    const data = await response.json();

    if (data.success) {
      if (!data.nodes || data.nodes.length === 0) {
        display.innerHTML = `
          <div class="text-center py-10 text-zinc-500">
            <i data-lucide="info" class="w-12 h-12 mx-auto mb-4 text-zinc-600"></i>
            <p class="font-medium mb-2">Cliente sem histórico</p>
            <p class="text-sm">${data.message || 'Este cliente ainda não assistiu filmes'}</p>
          </div>
        `;
        lucide.createIcons();
        return;
      }
      renderGraph(data, customerId);
      loadRecommendations(customerId);
    } else {
      display.innerHTML = `<div class="text-red-500 text-center">Erro: ${data.error}</div>`;
    }
  } catch (error) {
    display.innerHTML = `<div class="text-red-500 text-center">Erro: ${error.message}</div>`;
  }
}

function renderGraph(data, customerId) {
  const display = document.getElementById('graph-display');
  const { nodes, edges, total, showing } = data;

  const customer = nodes.find(n => n.type === 'customer');
  const movies = nodes.filter(n => n.type === 'movie');
  if (!customer) return;

  let html = '<div class="w-full space-y-6">';

  html += `
    <div class="bg-gradient-to-r from-indigo-600 to-purple-600 rounded-2xl p-6 shadow-xl">
      <div class="flex items-center gap-4">
        <div class="w-16 h-16 rounded-full bg-white flex items-center justify-center text-indigo-600 font-bold text-2xl">
          ${customer.label.charAt(0)}
        </div>
        <div>
          <div class="text-white font-bold text-2xl">${customer.label}</div>
          <div class="text-indigo-200 text-sm mt-1">
            ${total} filme(s) assistido(s) • Mostrando ${showing}
          </div>
        </div>
      </div>
    </div>
  `;

  html += '<div class="space-y-3">';
  movies.forEach((movie, i) => {
    html += `
      <div class="bg-zinc-900 border border-zinc-800 rounded-xl p-3 hover:border-red-500/50 transition-all">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-lg bg-red-600 flex items-center justify-center text-white font-bold flex-shrink-0">
            ${i + 1}
          </div>
          <div class="flex-1 text-white text-sm">${movie.label}</div>
          <i data-lucide="check-circle" class="w-4 h-4 text-emerald-500 flex-shrink-0"></i>
        </div>
      </div>
    `;
  });

  if (showing < total) {
    html += `
      <button onclick="loadCustomerGraph(${customerId}, ${showing + 20})" 
        class="w-full bg-zinc-800 hover:bg-zinc-700 border border-zinc-800 text-white py-3 rounded-xl transition-colors font-medium text-sm">
        Carregar mais ${Math.min(20, total - showing)} filmes
      </button>
    `;
  }

  html += '</div></div>';
  display.innerHTML = html;
  lucide.createIcons();
}

async function loadRecommendations(customerId) {
  try {
    const response = await fetch(`${API_BASE_URL}/graph/recommendations/${customerId}`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /graph/recommendations HTTP Error:', response.status, raw);
      return;
    }
    const data = await response.json();

    if (data.success && data.recommendations && data.recommendations.length > 0) {
      const container = document.getElementById('recommendations-display');
      const list = document.getElementById('recommendations-list');

      container.classList.remove('hidden');
      list.innerHTML = '';

      data.recommendations.forEach(rec => {
        list.innerHTML += `
          <div class="bg-zinc-900 border border-zinc-800 p-4 rounded-xl hover:border-red-500/30 transition-all">
            <h4 class="font-bold text-white mb-1 text-sm">${rec.title}</h4>
            <p class="text-xs text-zinc-400 mb-2 line-clamp-2">${rec.summary}</p>
            <p class="text-xs text-emerald-500">Similaridade: ${rec.similarity_score}</p>
          </div>
        `;
      });
      lucide.createIcons();
    }
  } catch (error) {
    console.error('Erro:', error);
  }
}

// ===== COMPARE =====
async function loadCustomersForCompare() {
  const sel1 = document.getElementById('compare-customer1');
  const sel2 = document.getElementById('compare-customer2');
  if (!sel1 || !sel2) return;

  sel1.innerHTML = '<option value="">Carregando...</option>';
  sel2.innerHTML = '<option value="">Carregando...</option>';

  try {
    const response = await fetch(`${API_BASE_URL}/customers`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers (compare) HTTP Error:', response.status, raw);
      sel1.innerHTML = '<option value="">Erro</option>';
      sel2.innerHTML = '<option value="">Erro</option>';
      return;
    }

    const data = await response.json();

    if (data.success && Array.isArray(data.data) && data.data.length > 0) {
      sel1.innerHTML = '';
      sel2.innerHTML = '';

      data.data.forEach(c => {
        const name = `${c.firstname} ${c.lastname}${c.email ? ' • ' + c.email : ''}`;
        sel1.innerHTML += `<option value="${c.id}">${name}</option>`;
        sel2.innerHTML += `<option value="${c.id}">${name}</option>`;
      });

      if (data.data.length >= 2) {
        sel1.value = data.data[0].id;
        sel2.value = data.data[1].id;
      }

      lucide.createIcons();
    } else {
      sel1.innerHTML = '<option value="">Nenhum cliente</option>';
      sel2.innerHTML = '<option value="">Nenhum cliente</option>';
    }
  } catch (e) {
    sel1.innerHTML = '<option value="">Erro</option>';
    sel2.innerHTML = '<option value="">Erro</option>';
  }
}

async function compareCustomers() {
  const id1 = document.getElementById('compare-customer1').value;
  const id2 = document.getElementById('compare-customer2').value;

  if (!id1 || !id2) { showToast('⚠ Selecione dois clientes!', 'error'); return; }
  if (id1 === id2) { showToast('⚠ Selecione clientes diferentes!', 'error'); return; }

  const result = document.getElementById('compare-result');
  result.classList.remove('hidden');
  result.innerHTML = '<div class="text-center py-10"><div class="animate-spin w-8 h-8 border-2 border-red-500 border-t-transparent rounded-full mx-auto"></div></div>';

  try {
    const response = await fetch(`${API_BASE_URL}/graph/compare/${id1}/${id2}`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /graph/compare HTTP Error:', response.status, raw);
      result.innerHTML = `<div class="text-red-500 text-center">Erro ${response.status}</div>`;
      return;
    }

    const data = await response.json();

    if (data.success) renderComparison(data);
    else result.innerHTML = `<div class="text-red-500 text-center">Erro: ${data.error || 'Falha na comparação'}</div>`;
  } catch (error) {
    result.innerHTML = '<div class="text-red-500 text-center">Erro ao comparar</div>';
  }
}

function renderComparison(data) {
  const result = document.getElementById('compare-result');
  const { customer1, customer2, common, similarity_score } = data;

  const commonCount = (common?.count || 0);

  const html = `
    <div class="bg-gradient-to-r from-purple-600 to-pink-600 rounded-2xl p-8 text-center mb-6">
      <div class="text-6xl font-bold text-white mb-2">${similarity_score}%</div>
      <div class="text-white/80 text-sm">Similaridade de Gostos</div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
      <div class="bg-indigo-900/20 border border-indigo-800 rounded-2xl p-6">
        <div class="text-center mb-4">
          <div class="w-16 h-16 rounded-full bg-indigo-600 flex items-center justify-center text-white font-bold text-2xl mx-auto mb-2">
            ${customer1.name.charAt(0)}
          </div>
          <h3 class="font-bold text-white">${customer1.name}</h3>
          <p class="text-sm text-zinc-400">${customer1.total_movies} filmes</p>
        </div>
        <div class="space-y-2">
          <p class="text-xs text-zinc-500 uppercase tracking-wider">Exclusivos:</p>
          ${(customer1.unique_movies || []).slice(0, 3).map(m => `
            <div class="text-xs text-zinc-300 bg-zinc-900 px-2 py-1 rounded">• ${m.title}</div>
          `).join('') || '<div class="text-xs text-zinc-500">Nenhum</div>'}
        </div>
      </div>

      <div class="bg-emerald-900/20 border border-emerald-800 rounded-2xl p-6">
        <div class="text-center mb-4">
          <div class="w-16 h-16 rounded-full bg-emerald-600 flex items-center justify-center mx-auto mb-2">
            <i data-lucide="git-merge" class="w-8 h-8 text-white"></i>
          </div>
          <h3 class="font-bold text-emerald-400">Em Comum</h3>
          <p class="text-sm text-zinc-400">${commonCount} filmes</p>
        </div>
        <div class="space-y-2 max-h-40 overflow-y-auto custom-scroll">
          ${(common?.movies || []).map(m => `
            <div class="text-xs text-zinc-300 bg-emerald-950 border border-emerald-900 px-2 py-1 rounded">✓ ${m.title}</div>
          `).join('') || '<div class="text-xs text-zinc-500">Nada em comum ainda</div>'}
        </div>
      </div>

      <div class="bg-purple-900/20 border border-purple-800 rounded-2xl p-6">
        <div class="text-center mb-4">
          <div class="w-16 h-16 rounded-full bg-purple-600 flex items-center justify-center text-white font-bold text-2xl mx-auto mb-2">
            ${customer2.name.charAt(0)}
          </div>
          <h3 class="font-bold text-white">${customer2.name}</h3>
          <p class="text-sm text-zinc-400">${customer2.total_movies} filmes</p>
        </div>
        <div class="space-y-2">
          <p class="text-xs text-zinc-500 uppercase tracking-wider">Exclusivos:</p>
          ${(customer2.unique_movies || []).slice(0, 3).map(m => `
            <div class="text-xs text-zinc-300 bg-zinc-900 px-2 py-1 rounded">• ${m.title}</div>
          `).join('') || '<div class="text-xs text-zinc-500">Nenhum</div>'}
        </div>
      </div>
    </div>

    <div class="bg-zinc-900 border border-zinc-800 rounded-2xl p-6">
      <h4 class="font-bold text-white mb-3 flex items-center gap-2">
        <i data-lucide="lightbulb" class="text-yellow-500"></i>
        Como o Property Graph Ajuda
      </h4>
      <p class="text-sm text-zinc-400 leading-relaxed">
        O grafo identificou <strong class="text-white">${commonCount} filme(s) em comum</strong> entre 
        ${customer1.name} e ${customer2.name}. Com base nessas conexões, podemos recomendar os filmes 
        exclusivos de um para o outro, criando sugestões altamente personalizadas.
      </p>
    </div>
  `;

  result.innerHTML = html;
  lucide.createIcons();
}

// ===== NETWORK 3D =====
async function loadCustomersForNetwork3D() {
  const select = document.getElementById('network3d-customer');
  if (!select) return;

  select.innerHTML = `<option value="">Carregando...</option>`;

  try {
    const response = await fetch(`${API_BASE_URL}/customers`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /customers (network3d) HTTP Error:', response.status, raw);
      select.innerHTML = `<option value="">Erro ao carregar clientes</option>`;
      return;
    }
    const data = await response.json();

    if (!data.success || !Array.isArray(data.data) || data.data.length === 0) {
      select.innerHTML = `<option value="">Nenhum cliente encontrado</option>`;
      return;
    }

    select.innerHTML = '';
    data.data.forEach(c => {
      const name = `${c.firstname} ${c.lastname}${c.email ? ' • ' + c.email : ''}`;
      select.innerHTML += `<option value="${c.id}">${name}</option>`;
    });

    select.value = data.data[0].id;
    lucide.createIcons();
  } catch (e) {
    select.innerHTML = `<option value="">Erro ao carregar clientes</option>`;
  }
}

function load3DGraphFromUI() {
  const customerId = document.getElementById('network3d-customer')?.value;
  const depth = parseInt(document.getElementById('network3d-depth')?.value || '2', 10);

  if (!customerId) { showToast('⚠ Escolha um cliente válido!', 'error'); return; }
  load3DGraph(customerId, depth);
}

async function load3DGraph(customerId, depth = 2) {
  const container = document.getElementById('graph-3d-container');
  const statsContainer = document.getElementById('graph-stats');

  container.innerHTML =
    '<div class="absolute inset-0 flex items-center justify-center"><div class="animate-spin w-12 h-12 border-4 border-red-500 border-t-transparent rounded-full"></div></div>';
  statsContainer.innerHTML = '';

  try {
    const response = await fetch(`${API_BASE_URL}/graph/network/${customerId}?depth=${depth}&limit=50`);
    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /graph/network HTTP Error:', response.status, raw);
      container.innerHTML = `<div class="absolute inset-0 flex items-center justify-center text-red-500">Erro ${response.status}</div>`;
      return;
    }

    const data = await response.json();

    if (!data.success) {
      container.innerHTML =
        `<div class="absolute inset-0 flex items-center justify-center text-red-500 text-center px-6">
          Erro ao carregar grafo<br/>
          <span class="text-xs text-zinc-400">${data.error || 'Sem detalhes'}</span>
        </div>`;
      return;
    }

    if (!data.nodes || data.nodes.length === 0) {
      container.innerHTML = `
        <div class="absolute inset-0 flex items-center justify-center text-zinc-400 text-center px-6">
          Nenhum dado para exibir.<br/>
          <span class="text-xs text-zinc-500">Esse cliente ainda não tem conexões suficientes no grafo.</span>
        </div>
      `;
      return;
    }

    const stats = data.stats || { total_nodes: data.nodes.length, total_links: (data.links || []).length, customers: 0, movies: 0 };
    statsContainer.innerHTML = `
      <div class="bg-zinc-950/50 border border-zinc-800 rounded-2xl p-4">
        <div class="text-[10px] uppercase tracking-wider text-zinc-500">Nós</div>
        <div class="text-2xl font-bold text-white">${stats.total_nodes}</div>
      </div>
      <div class="bg-zinc-950/50 border border-zinc-800 rounded-2xl p-4">
        <div class="text-[10px] uppercase tracking-wider text-zinc-500">Conexões</div>
        <div class="text-2xl font-bold text-white">${stats.total_links}</div>
      </div>
      <div class="bg-zinc-950/50 border border-zinc-800 rounded-2xl p-4">
        <div class="text-[10px] uppercase tracking-wider text-zinc-500">Clientes / Filmes</div>
        <div class="text-2xl font-bold text-white">${stats.customers} / ${stats.movies}</div>
      </div>
    `;

    container.innerHTML = '';
    Graph3D = ForceGraph3D()(container)
      .backgroundColor('rgba(0,0,0,0)')
      .showNavInfo(false)
      .nodeLabel(n => `${n.label || n.id}`)
      .nodeAutoColorBy('group')
      .nodeVal(n => n.size || 6)
      .linkWidth(l => (l.value || 1))
      .linkOpacity(0.45)
      .linkDirectionalParticles(2)
      .linkDirectionalParticleSpeed(0.005)
      .graphData({ nodes: data.nodes, links: data.links || [] })
      .onNodeClick(node => {
        const distance = 120;
        const distRatio = 1 + distance / Math.hypot(node.x, node.y, node.z);
        Graph3D.cameraPosition(
          { x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio },
          node,
          900
        );

        if (node.type === 'movie') {
          showToast(`🎬 ${node.label}`, 'info');
        } else if (node.type === 'customer') {
          showToast(`👤 ${node.label}`, 'info');
        }
      });

    setTimeout(() => {
      try {
        Graph3D.zoomToFit(900, 60);
      } catch (e) {}
    }, 200);

    lucide.createIcons();
  } catch (error) {
    console.error('❌ load3DGraph exception:', error);
    container.innerHTML = `<div class="absolute inset-0 flex items-center justify-center text-red-500">Erro ao carregar grafo 3D</div>`;
  }
}

function zoom3D(factor = 1.1) {
  if (!Graph3D) return;
  const cam = Graph3D.camera();
  if (!cam) return;

  cam.position.x *= factor;
  cam.position.y *= factor;
  cam.position.z *= factor;
  Graph3D.cameraPosition({ x: cam.position.x, y: cam.position.y, z: cam.position.z }, null, 150);
}

// ===== CHAT =====
async function handleChatSubmit(e) {
  e.preventDefault();
  const input = document.getElementById('chat-input');
  const msg = (input.value || '').trim();
  if (!msg) return;

  addChatMessage('user', msg);
  input.value = '';

  const typingId = addTypingBubble();

  try {
    const payload = { message: msg };
    if (currentCustomerId) payload.customer_id = currentCustomerId;

    const response = await fetch(`${API_BASE_URL}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      const raw = await response.text();
      console.error('❌ /chat HTTP Error:', response.status, raw);
      removeTypingBubble(typingId);
      addChatMessage('bot', `Ops… minha claquete caiu 😅 (Erro ${response.status}).`);
      return;
    }

    const data = await response.json();
    removeTypingBubble(typingId);

    if (data.success) {
      addChatMessage('bot', data.response || 'Sem resposta');

      if (Array.isArray(data.movie_cards) && data.movie_cards.length > 0) {
        addMovieCardsToChat(data.movie_cards, !!data.graph_used);
      }
    } else {
      addChatMessage('bot', `Hmm… deu ruim: ${data.error || 'sem detalhes'}`);
    }
  } catch (error) {
    console.error('❌ /chat exception:', error);
    removeTypingBubble(typingId);
    addChatMessage('bot', 'Falhei em falar com o servidor. Tenta de novo em alguns segundos 🙏');
  }
}

function addChatMessage(role, text) {
  const container = document.getElementById('chat-messages');
  const wrap = document.createElement('div');

  if (role === 'user') {
    wrap.className = 'self-end bg-red-600/20 border border-red-900/40 p-4 rounded-2xl rounded-tr-none max-w-[70%]';
    wrap.innerHTML = `<p class="text-sm leading-relaxed text-white">${escapeHtml(text)}</p>`;
  } else {
    wrap.className = 'bg-zinc-900/80 border border-zinc-800 p-4 rounded-2xl rounded-tl-none max-w-[70%]';
    wrap.innerHTML = `<p class="text-sm leading-relaxed text-zinc-100">${escapeHtml(text)}</p>`;
  }

  container.appendChild(wrap);
  container.scrollTop = container.scrollHeight;
  lucide.createIcons();
}

function addTypingBubble() {
  const container = document.getElementById('chat-messages');
  const el = document.createElement('div');
  const id = 'typing-' + Math.random().toString(16).slice(2);

  el.id = id;
  el.className = 'bg-zinc-900/80 border border-zinc-800 p-4 rounded-2xl rounded-tl-none max-w-[50%] flex items-center gap-3';
  el.innerHTML = `
    <div class="w-2 h-2 rounded-full bg-zinc-400" style="animation:pulse 1s infinite;"></div>
    <div class="w-2 h-2 rounded-full bg-zinc-400" style="animation:pulse 1s infinite; animation-delay:.15s;"></div>
    <div class="w-2 h-2 rounded-full bg-zinc-400" style="animation:pulse 1s infinite; animation-delay:.30s;"></div>
    <span class="text-xs text-zinc-500">CineBot pensando...</span>
  `;
  container.appendChild(el);
  container.scrollTop = container.scrollHeight;
  return id;
}

function removeTypingBubble(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

function addMovieCardsToChat(cards, graphUsed) {
  const container = document.getElementById('chat-messages');
  const wrap = document.createElement('div');
  wrap.className = 'bg-zinc-950/60 border border-zinc-800 p-4 rounded-2xl max-w-[92%]';

  wrap.innerHTML = `
    <div class="flex items-center justify-between mb-3">
      <div class="flex items-center gap-2">
        <i data-lucide="sparkles" class="text-red-500"></i>
        <div class="text-sm font-bold text-white">Sugestões${graphUsed ? ' (via Grafo)' : ''}</div>
      </div>
      <div class="text-[10px] text-zinc-500">${cards.length} card(s)</div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-3" id="chat-cards-grid"></div>
  `;

  container.appendChild(wrap);
  const grid = wrap.querySelector('#chat-cards-grid');

  cards.forEach(m => {
    const genres = (m.genres && typeof m.genres === 'object') ? Object.keys(m.genres).slice(0, 2).join(', ') : '';
    const poster = m.poster_url || `https://via.placeholder.com/300x450?text=${encodeURIComponent(m.title || 'Filme')}`;

    const card = document.createElement('div');
    card.className = 'bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden hover:border-red-500/30 transition-all';
    card.innerHTML = `
      <div class="h-28 bg-zinc-800 overflow-hidden">
        <img src="${poster}" class="w-full h-full object-cover" onerror="this.src='https://via.placeholder.com/300x450?text=No+Image'">
      </div>
      <div class="p-3">
        <div class="font-bold text-white text-sm truncate">${escapeHtml(m.title || '')}</div>
        <div class="text-[10px] text-zinc-500 mt-1">${escapeHtml(genres)}</div>
        <div class="text-xs text-zinc-400 mt-2 line-clamp-2">${escapeHtml((m.summary || '').slice(0, 120))}</div>
        <div class="mt-3 text-[10px] text-emerald-500">${escapeHtml(m.graph_reason || '')}</div>
      </div>
    `;
    grid.appendChild(card);
  });

  container.scrollTop = container.scrollHeight;
  lucide.createIcons();
}

// ===== TOAST =====
function showToast(message, type = 'success') {
  let toast = document.getElementById('toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'toast';
    toast.className = 'fixed bottom-6 right-6 z-[200] space-y-2';
    document.body.appendChild(toast);
  }

  const item = document.createElement('div');
  const base = 'px-4 py-3 rounded-xl shadow-2xl border text-sm flex items-center gap-2 max-w-sm';
  let style = 'bg-zinc-950 border-zinc-800 text-white';
  let icon = 'check-circle';

  if (type === 'error') { style = 'bg-red-950 border-red-900 text-red-100'; icon = 'x-circle'; }
  if (type === 'info') { style = 'bg-indigo-950 border-indigo-900 text-indigo-100'; icon = 'info'; }

  item.className = `${base} ${style}`;
  item.innerHTML = `<i data-lucide="${icon}" class="w-4 h-4"></i><span class="break-words">${escapeHtml(message)}</span>`;
  toast.appendChild(item);
  lucide.createIcons();

  setTimeout(() => item.classList.add('opacity-0'), 2600);
  setTimeout(() => item.remove(), 3100);
}

// ===== UTILS =====
function escapeHtml(str) {
  return String(str || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}
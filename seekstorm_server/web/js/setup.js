// Onboarding uses the same authenticated REST API as external clients.
let setupOpen = false;
let setupRequest = 0;
function sessionValue(name, value) {
    try {
        if (value === undefined) return sessionStorage.getItem(name);
        sessionStorage.setItem(name, value);
    } catch (_) { /* Storage may be disabled; this page still works. */ }
    return null;
}
const savedKey = sessionValue('seekpower.apiKey');
const savedIndex = sessionValue('seekpower.index');
if (savedKey !== null) API_KEY = savedKey;
if (savedIndex !== null && /^\d+$/.test(savedIndex)) {
    INDEX_ID = Number(savedIndex);
    STATUS_URL = `api/v1/index/${INDEX_ID}`;
    QUERY_URL = `${STATUS_URL}/query`;
}
function resetSearch() {
    resetDocumentEditor();
    status_loaded = false;
    $('#writeDocuments').prop('disabled', true);
    query = '';
    currPage = 1;
    currSort = 'score';
    maxPages = -1;
    TITLE_FIELD = 'title'; TEXT_FIELD = 'body'; URL_FIELD = 'url';
    IMAGE_FIELD = 'image'; DATE_FIELD = 'date'; IS_DATE_FIELD = false;
    search_mode = 'Lexical';
    $('#contentBox,#prevTitle,#prevText2,#searchError').empty();
    $('#prevText').attr('src', '').hide();
    $('#prevUrl,#splitter').hide();
    $('#searchInput,#searchMain,#queryVector,#dateFrom,#dateTo').val('');
    $('#vectorControls,#dateControls').prop('hidden', true);
    $('.switch-label').removeClass('active').filter('[data-index="2"]').addClass('active');
    $('input[name="search-type"][value="Lexical"]').prop('checked', true);
    $('.switch-selector').css('left', 'calc(66.66% + 4px)');
    updateTitle(true);
}
async function setupApi(method, path, key, body) {
    const response = await fetch(path, {
        method, headers: {apikey: key, 'Content-Type': 'application/json'},
        ...(body === undefined ? {} : {body: JSON.stringify(body)}), cache: 'no-store'
    });
    const text = await response.text();
    if (!response.ok) {
        if (response.status === 401) throw Object.assign(new Error('Invalid API key. Use an ordinary API key, not the master key.'), {status: response.status});
        throw Object.assign(new Error(text || `Request failed (${response.status}).`), {status: response.status});
    }
    const value = JSON.parse(text);
    if (value && typeof value === 'object' && 'Err' in value) throw Object.assign(new Error(value.Err), {status: response.status});
    return value && typeof value === 'object' && 'Ok' in value ? value.Ok : value;
}
function setSetupBusy(busy) {
    $('#welcome button,#welcome input,#welcome select,#welcome textarea').prop('disabled', busy);
    if (!busy) {
        $('#openIndex,#indexList').prop('disabled', !$('#indexList option').length);
        $('#vectorDimensions').prop('disabled', $('#indexMode').val() === 'Lexical');
    }
}
async function connectKey(key) {
    const request = ++setupRequest;
    $('#indexManager').prop('hidden', true);
    $('#setupError').text('');
    $('#indexList').empty();
    setSetupBusy(true);
    try {
        if (!key) throw new Error('Paste the Initial API key from the server logs, or another existing API key.');
        const indexes = await setupApi('GET', 'api/v1/apikey', key);
        if (request !== setupRequest) return;
        API_KEY = key;
        sessionValue('seekpower.apiKey', key);
        resetSearch();
        indexes.sort((a, b) => a.id - b.id).forEach(index => {
            $('<option>').val(String(index.id)).text(`${index.name} (ID ${index.id})`).appendTo('#indexList');
        });
        if (indexes.some(index => index.id === INDEX_ID)) $('#indexList').val(String(INDEX_ID));
        $('#indexListStatus').text(indexes.length ? `${indexes.length} index(es) available.` : 'No indexes yet. Create your first index below.');
        $('#indexManager').prop('hidden', false);
    } catch (error) {
        if (request === setupRequest) $('#setupError').text(error.message || 'Could not connect to the server.');
    } finally {
        if (request === setupRequest) setSetupBusy(false);
    }
}
function showSetup(connect = true) {
    setupOpen = true;
    $('#landingSearch,#landingMain,#documentPanel').hide();
    $('#writeDocuments').prop('disabled', true);
    $('#welcome').show();
    $('#setupKey').val(API_KEY);
    if (connect) connectKey(API_KEY);
}
function openSelectedIndex(id) {
    INDEX_ID = Number(id);
    STATUS_URL = `api/v1/index/${INDEX_ID}`;
    QUERY_URL = `${STATUS_URL}/query`;
    sessionValue('seekpower.index', String(INDEX_ID));
    resetSearch();
    const url = new URL(location.href);
    url.search = '';
    url.searchParams.set('index_id', String(INDEX_ID));
    history.replaceState(null, '', url);
    setupOpen = false;
    $('#welcome,#landingMain,#documentPanel').hide();
    $('#landingSearch').show();
    showStatus();
    $('#searchInput').trigger('focus');
}
function presetSchema() {
    const mode = $('#indexMode').val();
    const schema = [
        {field: 'title', field_type: 'Text', store: true, index_lexical: mode !== 'Vector'},
        {field: 'body', field_type: 'Text', store: true, index_lexical: mode !== 'Vector'},
        {field: 'url', field_type: 'Text', store: true, index_lexical: false}
    ];
    if (mode !== 'Lexical') schema.push({field: 'vector', field_type: 'Json', store: true, index_lexical: false, index_vector: true});
    $('#indexSchema').val(JSON.stringify(schema, null, 2));
    $('#dimensionControl').prop('hidden', mode === 'Lexical');
    $('#vectorDimensions').prop('disabled', mode === 'Lexical');
}
$(function () {
    presetSchema();
    $('#manageIndexes').on('click', () => showSetup());
    $('#connectForm').on('submit', event => { event.preventDefault(); connectKey($('#setupKey').val().trim()); });
    $('#forgetKey').on('click', () => {
        ++setupRequest;
        API_KEY = '';
        sessionValue('seekpower.apiKey', '');
        sessionValue('seekpower.index', '0');
        INDEX_ID = 0;
        resetSearch();
        const url = new URL(location.href); url.search = ''; history.replaceState(null, '', url);
        $('#indexManager').prop('hidden', true);
        $('#indexList').empty();
        $('#setupError').text('Key forgotten in this tab. The server key has not been revoked.');
        showSetup(false);
        $('#setupKey').trigger('focus');
    });
    $('#selectIndexForm').on('submit', event => {
        event.preventDefault();
        if ($('#indexList').val() !== null) openSelectedIndex($('#indexList').val());
    });
    $('#indexMode').on('change', presetSchema);
    $('#createIndexForm').on('submit', async event => {
        event.preventDefault();
        $('#setupError').text('');
        const request = ++setupRequest;
        setSetupBusy(true);
        try {
            const name = $('#newIndexName').val().trim();
            if (!name) throw new Error('Enter an index name.');
            const schema = JSON.parse($('#indexSchema').val());
            if (!Array.isArray(schema) || !schema.length) throw new Error('Schema must be a nonempty JSON array.');
            const fields = new Set();
            for (const field of schema) {
                if (!field || typeof field.field !== 'string' || !field.field.trim() || fields.has(field.field)) throw new Error('Each schema field needs a unique, nonempty name.');
                fields.add(field.field);
            }
            const mode = $('#indexMode').val();
            const body = {index_name: name, schema};
            if (mode !== 'Lexical') {
                const dimensions = Number($('#vectorDimensions').val());
                if (!Number.isInteger(dimensions) || dimensions < 1 || dimensions > 65535) throw new Error('Vector dimensions must be an integer from 1 to 65535.');
                if (!schema.some(field => field.index_vector)) throw new Error('Vector/Hybrid indexes need at least one index_vector field.');
                body.inference = {External: {dimensions, precision: 'F32', quantization: 'None', similarity: 'Cosine'}};
            } else if (schema.some(field => field.index_vector)) {
                throw new Error('Choose Vector or Hybrid to configure vector dimensions.');
            }
            const id = await setupApi('POST', 'api/v1/index', API_KEY, body);
            if (request === setupRequest) openSelectedIndex(id);
        } catch (error) {
            if (request === setupRequest) $('#setupError').text(error.message || 'Index creation failed.');
        } finally {
            if (request === setupRequest) setSetupBusy(false);
        }
    });
});

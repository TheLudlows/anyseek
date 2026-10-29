function newIdentity() {
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), x => x.toString(16).padStart(2, '0')).join('');
}
function newOperationId() { return {epoch: Math.floor(Date.now() / 86400000), nonce: newIdentity()}; }
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
if (savedIndex !== null && savedIndex.length > 0) {
    INDEX_ID = savedIndex;
    STATUS_URL = `api/v1/index/${encodeURIComponent(INDEX_ID)}`;
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
    $('#searchInput,#searchMain,#queryVector,#dateFrom,#dateTo,#eventFrom,#eventTo,#validAt').val('');
    $('#vectorControls,#dateControls').prop('hidden', true);
    $('.switch-label').removeClass('active').filter('[data-index="2"]').addClass('active');
    $('input[name="search-type"][value="Lexical"]').prop('checked', true);
    $('.switch-selector').css('left', 'calc(66.66% + 4px)');
    updateTitle(true);
}
async function setupApi(method, path, key, body) {
    return setupApiJson(method, path, key, body === undefined ? undefined : JSON.stringify(body));
}
// Accept already validated JSON text without rounding document number tokens.
async function setupApiJson(method, path, key, body) {
    const response = await fetch(path, {
        method, headers: {apikey: key, 'Content-Type': 'application/json'},
        ...(body === undefined ? {} : {body}), cache: 'no-store'
    });
    const value = await response.json();
    if (!response.ok) {
        if (response.status === 401) throw Object.assign(new Error('Invalid API key. Check the key copied from the server logs.'), {status: response.status, code: value.code});
        throw Object.assign(new Error(value.message || `Request failed (${response.status}).`), {status: response.status, code: value.code});
    }
    return value;
}
function setSetupBusy(busy) {
    $('#welcome button,#welcome input,#welcome select,#welcome textarea').prop('disabled', busy);
    if (!busy) {
        $('#openIndex,#indexList').prop('disabled', !$('#indexList option').length);
        $('#vectorDimensions').prop('disabled', $('#indexMode').val() === 'Lexical');
        if (sessionValue('seekpower.pendingTakeover')) $('#takeoverSource,#takeoverBackup,#takeoverTarget').prop('disabled', true);
        const pending = JSON.parse(sessionValue('seekpower.pendingCreate') || 'null');
        $('#discardRejectedCreation').prop('hidden', !pending?.rejected);
        if (pending) {
            const config = pending.body.config;
            $('#newIndexName').val(config.index_name);
            $('#indexSchema').val(JSON.stringify(config.schema, null, 2));
            $('#indexMode').val(config.vector ? (config.schema.some(f => f.field_type === 'Text' && f.index_lexical !== false) ? 'Hybrid' : 'Vector') : 'Lexical');
            if (config.vector) $('#vectorDimensions').val(config.vector.dimensions);
            $('#embeddingSpaceId').val(pending.body.embedding_space_id || '');
            $('#dimensionControl').prop('hidden', !config.vector);
            $('#createIndexForm input,#createIndexForm select,#createIndexForm textarea').prop('disabled', true);
            $('#createIndexForm button[type="submit"]').text('Retry original creation');
        } else {
            $('#createIndexForm button[type="submit"]').text('Create & open index');
        }
    }
}
async function connectKey(key) {
    const request = ++setupRequest;
    $('#indexManager').prop('hidden', true);
    $('#setupError').text('');
    $('#indexList').empty();
    setSetupBusy(true);
    try {
        if (!key) throw new Error('Paste the Master API key from the server logs, or another existing API key.');
        const indexes = await setupApi('GET', 'api/v1/apikey', key);
        if (request !== setupRequest) return;
        API_KEY = key;
        sessionValue('seekpower.apiKey', key);
        resetSearch();
        indexes.sort((a, b) => a.id.localeCompare(b.id)).forEach(index => {
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
    INDEX_ID = String(id);
    STATUS_URL = `api/v1/index/${encodeURIComponent(INDEX_ID)}`;
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
        {field: 'title', field_type: 'Text', ...(mode === 'Vector' ? {index_lexical: false} : {})},
        {field: 'body', field_type: 'Text', ...(mode === 'Vector' ? {index_lexical: false} : {})},
        {field: 'url', field_type: 'Text', index_lexical: false}
    ];
    if (mode !== 'Lexical') schema.push({field: 'vector', field_type: 'Json', index_vector: true});
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
    $('#serviceBackupId').val(sessionValue('seekpower.serviceBackupId') || '');
    for (const [button, action] of [['backupService', 'backup'], ['restoreService', 'restore']]) {
        $('#' + button).on('click', async () => {
            const slot = 'seekpower.pendingService' + action;
            try {
                let pending = JSON.parse(sessionValue(slot) || 'null');
                if (!pending) {
                    pending = {operation_id: newOperationId()};
                    if (action === 'restore') {
                        pending.backup_id = $('#serviceBackupId').val().trim();
                        if (!/^[0-9a-f]{64}$/.test(pending.backup_id)) throw new Error('Enter a lowercase service backup ID.');
                    }
                    sessionStorage.setItem(slot, JSON.stringify(pending));
                }
                $('#backupService,#restoreService').prop('disabled', true);
                const result = await setupApi('POST', action === 'backup' ? 'api/v1/service-backups' : 'api/v1/service-restores', API_KEY, pending);
                sessionStorage.removeItem(slot);
                if (action === 'backup') {
                    sessionValue('seekpower.serviceBackupId', result.backup_id);
                    $('#serviceBackupId').val(result.backup_id);
                    $('#serviceRecoveryStatus').text('Service backup created: ' + result.backup_id);
                } else {
                    $('#serviceRecoveryStatus').text('Restored to new stopped root: ' + result.target_root + '. Current master only; historical keys remain disabled. Source/evidence content remains quarantined until deletion-history reconciliation is available.');
                }
            } catch (error) {
                $('#serviceRecoveryStatus').text((error.message || 'Recovery request failed.') + ' Retry preserves the original request; no running service is replaced.');
            } finally { $('#backupService,#restoreService').prop('disabled', false); }
        });
    }
    const takeoverSlot = 'seekpower.pendingTakeover';
    const savedTakeover = JSON.parse(sessionValue(takeoverSlot) || 'null');
    if (savedTakeover) {
        $('#takeoverSource').val(savedTakeover.source_collection_id);
        $('#takeoverBackup').val(savedTakeover.backup_id);
        $('#takeoverTarget').val(savedTakeover.collection_id);
        $('#takeoverSource,#takeoverBackup,#takeoverTarget').prop('disabled', true);
    }
    $('#submitTakeover').on('click', async () => {
        try {
            let pending = JSON.parse(sessionValue(takeoverSlot) || 'null');
            if (!pending) {
                pending = {operation_id: newOperationId(), source_collection_id: $('#takeoverSource').val().trim(),
                    backup_id: $('#takeoverBackup').val().trim(), collection_id: $('#takeoverTarget').val().trim()};
                if (!pending.source_collection_id || !pending.collection_id || !/^[0-9a-f]{64}$/.test(pending.backup_id))
                    throw new Error('Enter source, new target and lowercase collection backup ID.');
                sessionStorage.setItem(takeoverSlot, JSON.stringify(pending));
            }
            $('#submitTakeover').prop('disabled', true);
            $('#takeoverSource,#takeoverBackup,#takeoverTarget').prop('disabled', true);
            const result = await setupApi('POST', 'api/v1/takeovers', API_KEY, pending);
            sessionStorage.removeItem(takeoverSlot);
            $('#takeoverSource,#takeoverBackup,#takeoverTarget').prop('disabled', false);
            $('#takeoverStatus').text('Recovery active: ' + result.collection_id + '. Original sealed; all acknowledged writes retained through sequence ' + result.certificate.final_seq + '. Old grants were not restored.');
        } catch (error) {
            $('#takeoverStatus').text((error.message || 'Recovery failed.') + ' Retry preserves the saved request; the original may already be sealed.');
        } finally { $('#submitTakeover').prop('disabled', false); }
    });
    $('#discardRejectedCreation').on('click', () => {
        const pending = JSON.parse(sessionValue('seekpower.pendingCreate') || 'null');
        if (!pending?.rejected) return;
        sessionStorage.removeItem('seekpower.pendingCreate');
        setSetupBusy(false);
        $('#setupError').text('Rejected creation discarded. Edit the configuration and submit again.');
    });
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
            const config = {index_name: name, schema};
            if (mode !== 'Lexical') {
                const dimensions = Number($('#vectorDimensions').val());
                if (!Number.isInteger(dimensions) || dimensions < 1 || dimensions > 4096) throw new Error('Vector dimensions must be an integer from 1 to 4096.');
                if (!schema.some(field => field.index_vector)) throw new Error('Vector/Hybrid indexes need at least one index_vector field.');
                config.vector = {dimensions};
            } else if (schema.some(field => field.index_vector)) {
                throw new Error('Choose Vector or Hybrid to configure vector dimensions.');
            }
            let pending = JSON.parse(sessionValue('seekpower.pendingCreate') || 'null');
            if (!pending) {
                const body = {operation_id: newOperationId(), collection_id: newIdentity(), config};
                if (mode !== 'Lexical') {
                    body.embedding_space_id = $('#embeddingSpaceId').val().trim();
                    if (!body.embedding_space_id) throw new Error('Enter the external embedding space/model identity.');
                }
                pending = {body};
                sessionStorage.setItem('seekpower.pendingCreate', JSON.stringify(pending));
            }
            const result = await setupApi('POST', 'api/v1/index', API_KEY, pending.body);
            sessionStorage.removeItem('seekpower.pendingCreate');
            if (request === setupRequest) openSelectedIndex(result.collection_id);
        } catch (error) {
            if (error.code === 'creation_rejected') {
                const pending = JSON.parse(sessionValue('seekpower.pendingCreate') || 'null');
                if (pending) { pending.rejected = true; sessionStorage.setItem('seekpower.pendingCreate', JSON.stringify(pending)); }
            }
            if (request === setupRequest) $('#setupError').text((error.message || 'Index creation failed.') + ' If creation is pending, retry submits its original identity and configuration.');
        } finally {
            if (request === setupRequest) setSetupBusy(false);
        }
    });
});

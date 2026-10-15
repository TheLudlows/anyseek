// JSON is parsed in the browser, then sent through the existing document API.
let CURRENT_SCHEMA = {};
let documentBusy = false;
let pendingDocumentCommit = null;
function resetDocumentEditor() {
    pendingDocumentCommit = null;
    CURRENT_SCHEMA = {};
    $('#documentJson,#documentFile').val('');
    $('#documentSchema,#documentIndex,#documentError,#documentStatus').text('');
    $('#retryDocumentCommit').prop('hidden', true);
}
function documentControls(busy) {
    documentBusy = busy;
    $('#manageIndexes,#writeDocuments,#backToSearch,#retryDocumentCommit').prop('disabled', busy);
    $('#writeDocuments').prop('disabled', busy || $('#documentPanel').is(':visible'));
    $('#documentJson,#documentFile,#submitDocuments').prop('disabled', busy || pendingDocumentCommit !== null);
}
function showDocuments() {
    if (documentBusy || !status_loaded || setupOpen) return;
    setupOpen = true; // Suspend search callbacks while editing records.
    pendingDocumentCommit = null;
    $('#landingSearch,#landingMain,#welcome').hide();
    $('#documentPanel').show();
    $('#documentIndex').text(`${$('#indexName').text()} (ID ${INDEX_ID})`);
    $('#documentSchema').text(JSON.stringify(Object.values(CURRENT_SCHEMA).map(field => ({
        field: field.field, type: field.field_type, stored: field.store,
        lexical: field.index_lexical, vector: field.index_vector || false
    })), null, 2));
    $('#documentJson,#documentFile').val('');
    $('#documentError,#documentStatus').text('');
    $('#retryDocumentCommit').prop('hidden', true);
    documentControls(false);
    $('#documentJson').trigger('focus');
}
async function commitDocuments() {
    const pending = pendingDocumentCommit;
    try {
        const count = await setupApi('PATCH', pending.path, pending.key);
        $('#documentStatus').text(`Wrote ${pending.count} document(s) and committed. Index now contains ${count} document(s).`);
        $('#indexedDocs').text(Number(count).toLocaleString());
        pendingDocumentCommit = null;
        $('#retryDocumentCommit').prop('hidden', true);
    } catch (error) {
        $('#documentError').text(`Records were accepted, but commit did not complete: ${error.message} Retry commit only; do not submit the records again.`);
        $('#retryDocumentCommit').prop('hidden', false);
    }
}
$(function () {
    $('#writeDocuments').on('click', showDocuments);
    $('#backToSearch').on('click', () => {
        if (!documentBusy) openSelectedIndex(INDEX_ID);
    });
    $('#documentFile').on('change', async function () {
        const file = this.files[0];
        if (!file) return;
        documentControls(true);
        $('#documentError,#documentStatus').text('');
        try { $('#documentJson').val(await file.text()); }
        catch (error) { $('#documentError').text(`Could not read file: ${error.message}`); }
        finally { documentControls(false); }
    });
    $('#documentsForm').on('submit', async event => {
        event.preventDefault();
        if (documentBusy || pendingDocumentCommit) return;
        $('#documentError,#documentStatus').text('');
        let records;
        try {
            records = JSON.parse($('#documentJson').val().replace(/^\uFEFF/, ''));
            const batch = Array.isArray(records) ? records : [records];
            if (!batch.length || !batch.every(record => record !== null && typeof record === 'object' && !Array.isArray(record))) {
                throw new Error('Enter one JSON object or a nonempty array of JSON objects.');
            }
        } catch (error) {
            $('#documentError').text(error.message);
            return;
        }
        documentControls(true);
        const path = `api/v1/index/${INDEX_ID}`;
        const key = API_KEY;
        let accepted = false;
        try {
            $('#documentStatus').text('Writing records…');
            await setupApi('POST', `${path}/doc`, key, records);
            accepted = true;
            pendingDocumentCommit = {path, key, count: Array.isArray(records) ? records.length : 1};
            $('#documentJson,#documentFile').val('');
            $('#documentStatus').text('Records accepted. Committing…');
            await commitDocuments();
        } catch (error) {
            $('#documentStatus').text('');
            $('#documentError').text(error.message + (error.status ? '' : ' The write outcome is unknown. Check the index before retrying: the server may have accepted the records.'));
        } finally {
            if (accepted && pendingDocumentCommit) $('#documentStatus').text('Records accepted; commit pending.');
            documentControls(false);
        }
    });
    $('#retryDocumentCommit').on('click', async () => {
        if (!pendingDocumentCommit || documentBusy) return;
        documentControls(true);
        $('#documentError').text('');
        await commitDocuments();
        documentControls(false);
    });
});

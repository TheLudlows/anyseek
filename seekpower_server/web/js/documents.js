// Each input object gets a stable identity before any request is sent. Pending
// requests survive reload; retry uses exactly the same operation and content.
let CURRENT_SCHEMA = {};
let documentBusy = false;
let pendingDocumentCommit = null;
function loadPendingWrites() {
    const pending = JSON.parse(sessionValue('seekpower.pendingWrites') || 'null');
    return pending && pending.collection === INDEX_ID ? pending : null;
}
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
    setupOpen = true;
    pendingDocumentCommit = loadPendingWrites();
    $('#landingSearch,#landingMain,#welcome').hide();
    $('#documentPanel').show();
    $('#documentIndex').text(`${$('#indexName').text()} (ID ${INDEX_ID})`);
    $('#documentSchema').text(JSON.stringify(Object.values(CURRENT_SCHEMA).map(field => ({field:field.field,type:field.field_type,stored:field.store,lexical:field.index_lexical,vector:field.index_vector||false})),null,2));
    $('#documentJson,#documentFile').val('');
    $('#documentError,#documentStatus').text('');
    $('#retryDocumentCommit').prop('hidden', !pendingDocumentCommit);
    if (pendingDocumentCommit) $('#documentStatus').text('Original write requests retained. Retry to resolve their outcomes.');
    documentControls(false);
}
async function submitPendingWrites() {
    const pending = pendingDocumentCommit;
    try {
        for (const request of pending.requests) {
            await setupApi('POST', `api/v1/index/${encodeURIComponent(pending.collection)}/doc`, API_KEY, request);
        }
        sessionStorage.removeItem('seekpower.pendingWrites');
        pendingDocumentCommit = null;
        $('#documentStatus').text(`Wrote ${pending.requests.length} record(s). All confirmed writes are durable and visible.`);
        $('#retryDocumentCommit').prop('hidden', true);
        $('#documentJson,#documentFile').val('');
    } catch (error) {
        $('#documentError').text(`${error.message} Original requests are retained; retry uses the same identities. An absent operation result does not prove failure.`);
        $('#retryDocumentCommit').prop('hidden', false);
        $('#documentStatus').text('Some outcomes may be unresolved. No batch atomicity is implied.');
    }
}
$(function () {
    $('#writeDocuments').on('click', showDocuments);
    $('#backToSearch').on('click', () => { if (!documentBusy) openSelectedIndex(INDEX_ID); });
    $('#documentFile').on('change', async function () {
        const file=this.files[0]; if(!file)return; documentControls(true);
        try {$('#documentJson').val(await file.text());} catch(error){$('#documentError').text(error.message);} finally{documentControls(false);}
    });
    $('#documentsForm').on('submit', async event => {
        event.preventDefault(); if(documentBusy||pendingDocumentCommit)return;
        $('#documentError,#documentStatus').text('');
        try {
            const input=JSON.parse($('#documentJson').val().replace(/^\uFEFF/,''));
            const documents=Array.isArray(input)?input:[input];
            if(!documents.length||!documents.every(d=>d&&typeof d==='object'&&!Array.isArray(d)))throw new Error('Enter one JSON object or a nonempty array of JSON objects.');
            if(sessionValue('seekpower.pendingWrites'))throw new Error('Resolve the retained writes in their original collection before submitting another batch.');
            pendingDocumentCommit={collection:INDEX_ID,requests:documents.map(document=>{
                const id=newIdentity(); return {operation_id:newOperationId(),record_id:id,routing_key:id,expected:'Absent',document};
            })};
            // Failure to retain identities must happen before any write is sent.
            sessionStorage.setItem('seekpower.pendingWrites',JSON.stringify(pendingDocumentCommit));
        } catch(error){pendingDocumentCommit=null;$('#documentError').text(error.message);return;}
        documentControls(true); await submitPendingWrites(); documentControls(false);
    });
    $('#retryDocumentCommit').on('click',async()=>{
        if(!pendingDocumentCommit||documentBusy)return;
        documentControls(true);$('#documentError').text('');await submitPendingWrites();documentControls(false);
    });
});

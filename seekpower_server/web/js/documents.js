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
    resetFeed();
    resetGraph();
    pendingDocumentCommit = null;
    CURRENT_SCHEMA = {};
    $('#documentJson,#documentFile').val('');
    $('#recordEnvelopeMode').prop('checked',false);
    $('#documentSchema,#documentIndex,#documentError,#documentStatus').text('');
    $('#retryDocumentCommit,#discardRejectedWrites').prop('hidden', true);
}
function documentControls(busy) {
    documentBusy = busy;
    $('#manageIndexes,#writeDocuments,#backToSearch,#retryDocumentCommit,#discardRejectedWrites').prop('disabled', busy);
    $('#writeDocuments').prop('disabled', busy || $('#documentPanel').is(':visible'));
    $('#documentJson,#documentFile,#recordEnvelopeMode,#submitDocuments').prop('disabled', busy || pendingDocumentCommit !== null);
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
    $('#discardRejectedWrites').prop('hidden', !pendingDocumentCommit?.rejected);
    if (pendingDocumentCommit) $('#documentStatus').text('Original write requests retained. Retry to resolve their outcomes.');
    documentControls(false);
}
async function submitPendingWrites() {
    const pending = pendingDocumentCommit;
    // Older saved requests without this flag may already have been sent.
    let previouslyUnresolved = pending.unresolved ?? !pending.rejected;
    try {
        for (let i = pending.completed || 0; i < pending.requests.length; i++) {
            const request = pending.requests[i];
            previouslyUnresolved = pending.unresolved ?? !pending.rejected;
            pending.unresolved = true;
            pending.rejected = false;
            // A reload or disconnect during fetch must retain uncertainty. If
            // retaining this state fails, do not send another request.
            sessionStorage.setItem('seekpower.pendingWrites', JSON.stringify(pending));
            await setupApi('POST', `api/v1/index/${encodeURIComponent(pending.collection)}/doc`, API_KEY, request);
            pending.completed = i + 1; pending.unresolved = false;
            sessionStorage.setItem('seekpower.pendingWrites', JSON.stringify(pending));
        }
        sessionStorage.removeItem('seekpower.pendingWrites');
        pendingDocumentCommit = null;
        $('#documentStatus').text(pending.requests.some(r => r.generation?.action === 'stage')
            ? `Wrote ${pending.requests.length} record(s). Staging is durable, not searchable until an explicit publish succeeds.`
            : `Wrote ${pending.requests.length} record(s). All confirmed writes are durable and visible.`);
        $('#retryDocumentCommit,#discardRejectedWrites').prop('hidden', true);
        $('#documentJson,#documentFile').val('');
    } catch (error) {
        // A rejected retry only describes this attempt; admission may reject
        // before the server looks up an earlier, already committed operation.
        pending.rejected = !previouslyUnresolved && (error.code === 'rejected' || error.code === 'conflict');
        pending.unresolved = !pending.rejected;
        try { sessionStorage.setItem('seekpower.pendingWrites', JSON.stringify(pending)); } catch (_) {}
        $('#discardRejectedWrites').prop('hidden', !pending.rejected);
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
            pendingDocumentCommit={collection:INDEX_ID,unresolved:false,requests:documents.map(document=>{
                if ($('#recordEnvelopeMode').prop('checked')) {
                    if ((!Object.hasOwn(document,'document') && !Object.hasOwn(document,'vector_patch') && !Object.hasOwn(document,'generation')) || !document.record_id || !document.routing_key || !document.expected)
                        throw new Error('Each envelope requires record_id, routing_key, expected and explicit document (null deletes) or vector_patch/generation.');
                    if (Object.hasOwn(document,'operation_id')) throw new Error('Operation IDs are managed by this editor.');
                    return {...document, operation_id:newOperationId()};
                }
                const id=newIdentity(); return {operation_id:newOperationId(),record_id:id,routing_key:id,expected:'Absent',document};
            })};
            // Failure to retain identities must happen before any write is sent.
            sessionStorage.setItem('seekpower.pendingWrites',JSON.stringify(pendingDocumentCommit));
        } catch(error){pendingDocumentCommit=null;$('#documentError').text(error.message);return;}
        documentControls(true); await submitPendingWrites(); documentControls(false);
    });
    $('#discardRejectedWrites').on('click', () => {
        if (documentBusy || !pendingDocumentCommit?.rejected) return;
        const completed = pendingDocumentCommit.completed || 0;
        sessionStorage.removeItem('seekpower.pendingWrites');
        pendingDocumentCommit = null;
        $('#retryDocumentCommit,#discardRejectedWrites').prop('hidden', true);
        $('#documentJson,#documentFile').val(''); $('#documentError').text('');
        $('#documentStatus').text(`${completed} confirmed writes remain saved. Rejected and unsent requests discarded; enter only the records you still want to write.`);
        documentControls(false);
    });
    $('#retryDocumentCommit').on('click',async()=>{
        if(!pendingDocumentCommit||documentBusy)return;
        documentControls(true);$('#documentError').text('');await submitPendingWrites();documentControls(false);
    });
});

// Bounded inspection UI, not a background synchronizer. Never retain bodies or
// credentials in storage; errors invalidate partial snapshots and continuations.
let feedState = null;
let feedBusy = false;
function resetFeed() {
    feedState = null;
    $('#feedResult,#feedStatus').text('');
    $('#feedNext').prop('disabled',true);
}
async function inspectFeed(action) {
    if (feedBusy) return;
    feedBusy=true;$('#feedBootstrap,#feedNext,#feedRead').prop('disabled',true);
    const collection=INDEX_ID,key=API_KEY,base=`api/v1/index/${encodeURIComponent(collection)}`;
    const current=()=>collection===INDEX_ID && key===API_KEY;
    try {
        if (!feedState || feedState.collection!==collection || feedState.key!==key) resetFeed();
        if (action==='start') {
            if(feedState?.id||feedState?.cleanup) await setupApi('DELETE',`${base}/bootstrap/${feedState.id||feedState.cleanup}`,key);
            resetFeed();
            const identity=await setupApi('GET',`${base}/bootstrap`,key);
            if(!current())return;
            feedState={collection,key,id:identity.request_id_prefix+newIdentity().replaceAll('-','').slice(0,16),after:null,cursor:null};
        }
        if(action==='start'||action==='next') {
            if(!feedState?.id)throw new Error('Start a new snapshot.');
            const page=await setupApi('POST',`${base}/bootstrap`,key,{request_id:feedState.id,after:feedState.after,length:10});
            if(!current())return;
            if(feedState.cut!==undefined && feedState.cut!==page.visible_seq)throw new Error('Snapshot cut changed; rebuild required.');
            feedState.cut=page.visible_seq;feedState.after=page.next;feedState.retry=false;
            $('#feedResult').text(JSON.stringify(page,null,2));
            if(!page.next) {
                feedState.cursor=page.continuation;
                const completedId=feedState.id;
                feedState.id=null;
                try {await setupApi('DELETE',`${base}/bootstrap/${completedId}`,key);}
                catch(error) {
                    if([401,403,410].includes(error.status))throw error;
                    if(current())feedState.cleanup=completedId;
                }
                if(!current())return;
            }
            $('#feedStatus').text(page.next?'Snapshot page ready. Continue before its lease expires.':`Snapshot complete; continuation ready for changes.${feedState.cleanup?' Release unconfirmed; starting a new snapshot retries cleanup.':''}`);
        } else {
            const page=await setupApi('POST',`${base}/changes`,key,{after:feedState?.cursor||null,length:100});
            if(!current())return;
            if(!feedState)feedState={collection,key};
            feedState.cursor=page.next;
            $('#feedResult').text(JSON.stringify(page,null,2));
            $('#feedStatus').text(page.events.some(e=>e.rescan_required)?'Invalidation received: rebuild your effective view with a new snapshot.':'Change page ready.');
        }
    } catch(error) {
        if(current()) {
            $('#feedResult').text('');
            if ([401,403,410].includes(error.status)) {
                resetFeed();$('#feedStatus').text(`${error.message} Discard partial snapshot and start again.`);
            } else {
                // Preserve the exact snapshot request after transport loss.
                // A retry cannot silently allocate a different snapshot cut.
                if(feedState?.id)feedState.retry=true;
                $('#feedStatus').text(`${error.message} Retry the snapshot page or read changes again.`);
            }
        }
    } finally {
        feedBusy=false;$('#feedBootstrap,#feedRead').prop('disabled',false);
        $('#feedNext').prop('disabled',!feedState?.after && !feedState?.retry).text(feedState?.retry?'Retry snapshot page':'Next snapshot page');
    }
}
$(function(){
    $('#feedBootstrap').on('click',()=>inspectFeed('start'));
    $('#feedNext').on('click',()=>inspectFeed('next'));
    $('#feedRead').on('click',()=>inspectFeed('changes'));
});

let graphState=null;
let graphBusy=false;
function resetGraph(){graphState=null;$('#relationResult,#relationStatus').text('');$('#nextEntities').prop('disabled',true);}
async function inspectGraph(action){
    if(graphBusy)return;graphBusy=true;
    $('#listEntities,#nextEntities,#expandNeighbors').prop('disabled',true);
    const collection=INDEX_ID,key=API_KEY,base=`api/v1/index/${encodeURIComponent(collection)}`;
    const current=()=>collection===INDEX_ID&&key===API_KEY;
    try{
        if(!graphState||graphState.collection!==collection||graphState.key!==key)resetGraph();
        let result;
        if(action==='neighbors'){
            result=await setupApi('POST',`${base}/neighbors`,key,{seeds:JSON.parse($('#relationSeeds').val()),direction:$('#relationDirection').val(),relation_types:JSON.parse($('#relationTypes').val()),per_seed:Number($('#relationLimit').val()),max_candidates:Number($('#relationBudget').val()),allow_partial:$('#relationPartial').prop('checked')});
            if(!current())return;
            $('#relationStatus').text(`${result.count.relation}: ${result.count.value} visible paths; view ${result.visible_seq}, evaluation ${result.evaluation_time_ms}.${result.complete?'':` Incomplete: ${result.reason}.`}`);
        }else{
            result=await setupApi('POST',`${base}/entities`,key,{cursor:action==='next'?graphState?.next:null,length:20});
            if(!current())return;
            graphState={collection,key,next:result.next};
            $('#relationStatus').text(`Entity page; view ${result.visible_seq}, evaluation ${result.evaluation_time_ms}.`);
        }
        $('#relationResult').text(JSON.stringify(result,null,2));
    }catch(error){if(current()){resetGraph();$('#relationStatus').text(error.message);}}
    finally{graphBusy=false;$('#listEntities,#expandNeighbors').prop('disabled',false);$('#nextEntities').prop('disabled',!graphState?.next);}
}
$(function(){
    $('#listEntities').on('click',()=>inspectGraph('entities'));
    $('#nextEntities').on('click',()=>inspectGraph('next'));
    $('#expandNeighbors').on('click',()=>inspectGraph('neighbors'));
});

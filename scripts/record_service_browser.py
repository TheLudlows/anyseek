"""Real Chromium checks for the retained UI, through the default service API."""
import json, time, os, pathlib
from playwright.sync_api import sync_playwright, expect

def exercise(base,master,call):
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        context=browser.new_context(viewport={'width':1100,'height':900})
        page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
        page.goto(base);page.locator('#manageIndexes').click()
        page.locator('#setupKey').fill(master);page.locator('#connectForm button[type="submit"]').click()
        expect(page.locator('#indexManager')).to_be_visible()
        # A known pre-prepare rejection is editable after reload; uncertainty is not.
        page.locator('#newIndexName').fill('rejected-config')
        page.locator('#createIndexForm details summary').click()
        schema=[{'field':'body','field_type':'Text','store':True,'boost':0}]
        page.locator('#indexSchema').fill(json.dumps(schema))
        page.locator('#createIndexForm button[type="submit"]').click()
        expect(page.locator('#discardRejectedCreation')).to_be_visible()
        page.reload(wait_until="domcontentloaded");page.locator('#manageIndexes').click()
        expect(page.locator('#discardRejectedCreation')).to_be_visible()
        page.locator('#discardRejectedCreation').click()
        expect(page.locator('#newIndexName')).to_be_enabled()
        page.locator('#indexMode').select_option('Vector');page.locator('#indexMode').select_option('Lexical')
        page.locator('#newIndexName').fill('browser-records')
        creations=[]
        def lose_creation(route):
            creations.append(route.request.post_data_json)
            reply=route.fetch();assert reply.status==200
            route.abort('failed')
        page.route('**/api/v1/index',lose_creation,times=1)
        page.locator('#createIndexForm button[type="submit"]').click()
        expect(page.locator('#newIndexName')).to_be_disabled()
        assert page.evaluate("JSON.parse(sessionStorage.getItem('seekpower.pendingCreate')).body")==creations[0]
        expect(page.locator('#discardRejectedCreation')).to_be_hidden()
        page.reload(wait_until="domcontentloaded");page.locator('#manageIndexes').click()
        page.locator('#createIndexForm button[type="submit"]').click()
        expect(page.locator('#indexName')).to_have_text('browser-records')
        collection=page.evaluate('INDEX_ID')
        assert isinstance(collection,str) and len(collection)==32
        page.locator('#writeDocuments').click()
        documents=[{'title':'Browser title','body':'browser alpha beta','url':'https://example.com'}]
        page.locator('#documentJson').fill(json.dumps(documents))
        writes=[]
        def lose_response(route):
            writes.append(route.request.post_data_json)
            # Send the real request to the real server, then lose its response.
            reply=route.fetch();assert reply.status==200
            route.abort('failed')
        page.route('**/api/v1/index/*/doc',lose_response,times=1)
        page.locator('#submitDocuments').click()
        expect(page.locator('#retryDocumentCommit')).to_be_visible()
        expect(page.locator('#documentJson')).to_be_disabled()
        saved=page.evaluate("JSON.parse(sessionStorage.getItem('seekpower.pendingWrites'))")
        assert [json.loads(request['body']) for request in saved['requests']]==writes and 'key' not in saved
        page.reload(wait_until="domcontentloaded");expect(page.locator('#indexName')).to_have_text('browser-records')
        page.locator('#writeDocuments').click();expect(page.locator('#retryDocumentCommit')).to_be_visible()
        # The first request really committed. A later admission rejection or
        # conflict cannot prove it failed, including after another reload.
        for status,code in [(400,'rejected'),(409,'conflict'),(429,'busy')]:
            def reject_retry(route,request,status=status,code=code):
                route.fulfill(status=status,content_type='application/json',body=json.dumps({'code':code,'message':'retry '+code}))
            page.route('**/api/v1/index/*/doc',reject_retry,times=1)
            page.locator('#retryDocumentCommit').click()
            expect(page.locator('#documentError')).to_contain_text('retry '+code)
            expect(page.locator('#discardRejectedWrites')).to_be_hidden()
            expect(page.locator('#documentJson')).to_be_disabled()
            retained=page.evaluate("JSON.parse(sessionStorage.getItem('seekpower.pendingWrites'))")
            assert retained['unresolved'] and [json.loads(request['body']) for request in retained['requests']]==writes
            page.reload(wait_until="domcontentloaded")
            expect(page.locator('#indexName')).to_have_text('browser-records')
            page.locator('#writeDocuments').click()
            expect(page.locator('#discardRejectedWrites')).to_be_hidden()
        page.locator('#retryDocumentCommit').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        assert page.evaluate("sessionStorage.getItem('seekpower.pendingWrites')") is None
        assert call('GET',f'/api/v1/index/{collection}')['indexed_doc_count']==1
        page.locator('summary').filter(has_text='Change feed and snapshot bootstrap').click()
        def lose_bootstrap(route):
            response=route.fetch();assert response.status==200
            route.abort('failed')
        page.route('**/api/v1/index/*/bootstrap',lambda route: lose_bootstrap(route) if route.request.method=='POST' else route.continue_())
        page.locator('#feedBootstrap').click()
        expect(page.locator('#feedStatus')).to_contain_text('Retry the snapshot page')
        page.unroute('**/api/v1/index/*/bootstrap')
        def lose_release(route):
            response=route.fetch();assert response.status==200
            route.abort('failed')
        page.route('**/api/v1/index/*/bootstrap/*',lose_release,times=1)
        page.locator('#feedNext').click()
        expect(page.locator('#feedStatus')).to_contain_text('Snapshot complete')
        expect(page.locator('#feedStatus')).to_contain_text('Release unconfirmed')
        expect(page.locator('#feedNext')).to_be_disabled()
        assert 'continuation' in page.locator('#feedResult').inner_text()
        page.locator('#feedRead').click()
        expect(page.locator('#feedStatus')).to_contain_text('Change page ready')
        print('PASS Chromium: real snapshot bootstrap and feed continuation')
        graph_records=[{'record_id':rid,'routing_key':'graph-route','expected':'Absent','document':{'body':'graphnode'},'system':{'kind':'entity'}} for rid in ['graph-a','graph-b']]
        graph_records.append({'record_id':'graph-edge','routing_key':'graph-route','expected':'Absent','document':{'body':'graphlink'},'system':{'kind':'relation','relation':{'from':{'record_id':'graph-a','routing_key':'graph-route','version':1},'to':{'record_id':'graph-b','routing_key':'graph-route','version':1},'relation_type':'knows'}}})
        page.locator('#recordEnvelopeMode').check();page.locator('#documentJson').fill(json.dumps(graph_records));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('Wrote 3 record(s)')
        page.locator('summary').filter(has_text='Entities and explicit one-hop relations').click()
        page.locator('#listEntities').click();expect(page.locator('#relationStatus')).to_contain_text('Entity page')
        assert 'graph-a' in page.locator('#relationResult').inner_text()
        page.locator('#relationSeeds').fill(json.dumps([{'record_id':'graph-a','routing_key':'graph-route','version':1}]))
        page.locator('#expandNeighbors').click();expect(page.locator('#relationStatus')).to_contain_text('exact: 1 visible paths')
        assert 'graph-b' in page.locator('#relationResult').inner_text()
        page.locator('#fusionQuery').fill(json.dumps({'query':'graphlink','length':10,'relation_fusion':{'rrf':{'constant':3,'keyword_weight':1,'vector_weight':0,'keyword_candidates':100},'relation_weight':1}}))
        page.locator('#fuseRelations').click();expect(page.locator('#relationStatus')).to_contain_text('Candidate window:')
        fusion=json.loads(page.locator('#relationResult').inner_text())
        assert fusion['fusion']['count_relation']=='candidate_window'
        assert any(hit['record']['record_id']=='graph-b' for hit in fusion['hits'])

        page.locator('#recordEnvelopeMode').uncheck()
        print('PASS Chromium: entity/edge envelopes, entity directory and real bounded one-hop')
        page.locator('#backToSearch').click()
        page.locator('#searchInput').fill('browser alpha')
        expect(page.locator('#contentBox')).to_contain_text('Browser title')
        page.reload(wait_until="domcontentloaded");expect(page.locator('#indexName')).to_have_text('browser-records')
        # New source/evidence path through the actual editor, with stable IDs.
        page.locator('#writeDocuments').click()
        page.locator('#recordEnvelopeMode').check()
        envelopes=[
            {'record_id':'ui-source','routing_key':'ui-route','expected':'Absent','document':{'title':'Source title','body':'sourceword'},'system':{'kind':'source'}},
            {'record_id':'ui-derived','routing_key':'ui-route','expected':'Absent','document':{'title':'Derived title','body':'sourceword'},'system':{'kind':'derived','evidence_refs':[{'record_id':'ui-source','routing_key':'ui-route','version':1}]}},
        ]
        page.locator('#documentJson').fill(json.dumps(envelopes))
        page.locator('#submitDocuments').click();expect(page.locator('#documentStatus')).to_contain_text('Wrote 2 record(s)')
        page.locator('summary').filter(has_text='Source evidence excerpts').click()
        page.locator('#evidenceRef').fill(json.dumps({'record_id':'ui-source','routing_key':'ui-route','version':1,'locator':{'kind':'utf8','field':'body','start':0,'end':6}}))
        page.locator('#readEvidence').click();expect(page.locator('#evidenceResult')).to_contain_text('content_sha256')
        expect(page.locator('#evidenceResult')).to_contain_text('source')
        print('PASS Chromium: exact source excerpt and content hash')
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('sourceword')
        expect(page.locator('#contentBox')).to_contain_text('Derived title')
        page.locator('#writeDocuments').click();page.locator('#recordEnvelopeMode').check()
        revision={'record_id':'ui-source','routing_key':'ui-route','expected':{'Match':1},'document':{'title':'Updated source','body':'sourceword revised'}}
        page.locator('#documentJson').fill(json.dumps(revision));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('Wrote 1 record(s)')
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-derived/status')['visibility']=='stale'
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-derived',status=404)['code']=='not_found'
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('sourceword')
        expect(page.locator('#contentBox')).to_contain_text('Updated source')
        expect(page.locator('#contentBox')).not_to_contain_text('Derived title')
        page.locator('#writeDocuments').click();page.locator('#recordEnvelopeMode').check()
        generation={'record_id':'ui-generation','routing_key':'ui-route','expected':'Absent','generation':{'action':'stage','generation_id':'g1','source':{'record_id':'ui-source','routing_key':'ui-route','version':2},'members':['ui-ga','ui-gb'],'chunks':[{'record_id':'ui-ga','document':{'title':'Generation A','body':'generationui'}},{'record_id':'ui-gb','document':{'title':'Generation B','body':'generationui'}}]}}
        page.locator('#documentJson').fill(json.dumps(generation));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('not searchable')
        assert call('POST',f'/api/v1/index/{collection}/query',{'query':'generationui'})['count_total']==0
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-generation/generation')['staged_members']==2
        page.locator('summary').filter(has_text='Staging lease status').click()
        page.locator('#leaseDocument').fill('ui-generation');page.locator('#inspectLease').click()
        expect(page.locator('#leaseResult')).to_contain_text('lease_expires_at_ms')
        renewal={'record_id':'ui-generation','routing_key':'ui-route','expected':{'Match':1},'generation':{'action':'renew','generation_id':'g1'}}
        page.locator('#documentJson').fill(json.dumps(renewal));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('not searchable')
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-generation/generation')['lease_fence']==2
        print('PASS Chromium: staging lease inspection and durable renewal without publication')
        publish={'record_id':'ui-generation','routing_key':'ui-route','expected':{'Match':2},'generation':{'action':'publish','generation_id':'g1'}}
        page.locator('#documentJson').fill(json.dumps(publish));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        upload={'record_id':'ui-upload','routing_key':'ui-route','expected':'Absent','generation':{'action':'begin','generation_id':'g','source':{'record_id':'ui-source','routing_key':'ui-route','version':2},'members':['ui-up-a','ui-up-b']}}
        page.locator('#documentJson').fill(json.dumps(upload));page.locator('#submitDocuments').click();expect(page.locator('#documentStatus')).to_contain_text('Staging is durable')
        for version,rid in [(1,'ui-up-b'),(2,'ui-up-a')]:
            upload={'record_id':'ui-upload','routing_key':'ui-route','expected':{'Match':version},'generation':{'action':'stage_batch','generation_id':'g','chunks':[{'record_id':rid,'document':{'title':rid,'body':'multipartui'}}]}}
            page.locator('#documentJson').fill(json.dumps(upload));page.locator('#submitDocuments').click();expect(page.locator('#documentStatus')).to_contain_text('Staging is durable')
            assert call('POST',f'/api/v1/index/{collection}/query',{'query':'multipartui'})['count_total']==0
        status=call('GET',f'/api/v1/index/{collection}/doc/ui-upload/generation')
        for action,version,extra in [('seal',3,{'manifest_sha256':status['manifest_sha256']}),('publish',4,{})]:
            upload={'record_id':'ui-upload','routing_key':'ui-route','expected':{'Match':version},'generation':{'action':action,'generation_id':'g',**extra}}
            page.locator('#documentJson').fill(json.dumps(upload));page.locator('#submitDocuments').click()
            expect(page.locator('#documentStatus')).to_contain_text('Staging is durable' if action=='seal' else 'All confirmed writes are durable and visible')
        assert call('POST',f'/api/v1/index/{collection}/query',{'query':'multipartui'})['count_total']==2
        print('PASS Chromium: multipart begin/two batches/seal/publish, staged invisibility and complete publication')
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('generationui')
        expect(page.locator('#contentBox')).to_contain_text('Generation A');expect(page.locator('#contentBox')).to_contain_text('Generation B')
        explanation=page.locator('.hitExplanation').first;explanation.locator('summary').click()
        displayed=json.loads(explanation.locator('pre').inner_text());assert displayed['channels'][0]['channel']=='lexical' and displayed['channels'][0]['raw_score']==displayed['score']
        print('PASS Chromium: real per-hit raw-score and channel explanation')

        page.locator('#writeDocuments').click();page.locator('#recordEnvelopeMode').check()
        deletion={'record_id':'ui-source','routing_key':'ui-route','expected':{'Match':2},'document':None}
        page.locator('#documentJson').fill(json.dumps(deletion));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('Wrote 1 record(s)')
        assert call('POST',f'/api/v1/index/{collection}/query',{'query':'sourceword'})['hits']==[]
        assert call('POST',f'/api/v1/index/{collection}/query',{'query':'generationui'})['count_total']==0
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-derived',status=404)['code']=='not_found'
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('sourceword')
        expect(page.locator('#contentBox')).not_to_contain_text('Derived title')
        print('PASS Chromium: source/derived envelopes, source deletion hides derived search and original reads')
        # Explicit TTL metadata is preserved by the real editor; temporal filters
        # go through the visible controls and the actual query endpoint.
        import time
        page.locator('#writeDocuments').click();page.locator('#recordEnvelopeMode').check()
        temporal={'record_id':'ui-time','routing_key':'ui-route','expected':'Absent','document':{'title':'Temporal title','body':'temporalui'},'temporal':{'event_time_ms':100,'valid_from_ms':100,'valid_to_ms':200,'expire_at_ms':int(time.time()*1000)+86400000}}
        page.locator('#documentJson').fill(json.dumps(temporal));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('temporalui')
        expect(page.locator('#contentBox')).to_contain_text('Temporal title')
        page.locator('#timeControls summary').click();page.locator('#eventFrom').fill('100');page.locator('#eventTo').fill('101');page.locator('#validAt').fill('200');page.locator('#timeApply').click()
        expect(page.locator('#contentBox')).not_to_contain_text('Temporal title')
        page.locator('#validAt').fill('100');page.locator('#timeApply').click()
        expect(page.locator('#contentBox')).to_contain_text('Temporal title');expect(page.locator('#evaluationTime')).to_contain_text('Evaluated at')
        assert call('GET',f'/api/v1/index/{collection}/doc/ui-time')['temporal']==temporal['temporal']
        page.locator('#timeClear').click()
        print('PASS Chromium: temporal envelope, half-open event/valid filters and fixed evaluation time')
        # Keep existing Vector/Hybrid setup, with explicit model/space identity.
        for mode in ['Vector','Hybrid']:
            page.locator('#manageIndexes').click();expect(page.locator('#newIndexName')).to_be_enabled()
            page.locator('#indexMode').select_option(mode);page.locator('#newIndexName').fill('browser-'+mode)
            page.locator('#vectorDimensions').fill('2');page.locator('#embeddingSpaceId').fill('browser-model-v1')
            page.locator('#createIndexForm button[type="submit"]').click();expect(page.locator('#indexName')).to_have_text('browser-'+mode)
            page.locator('#writeDocuments').click();page.locator('#documentJson').fill(json.dumps({'title':mode,'body':'alpha','vector':[2,0]}))
            page.locator('#submitDocuments').click();expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
            if mode=='Vector':
                vector_collection=page.evaluate('INDEX_ID')
                browse=call('POST',f'/api/v1/index/{vector_collection}/query',{})
                assert browse['count_total']==1 and browse['hits'][0]['record']['document']['title']=='Vector',browse
                count=call('POST',f'/api/v1/index/{vector_collection}/query',{'length':0})
                assert count['count_total']==1 and count['hits']==[],count
            page.locator('#recordEnvelopeMode').check()
            pending={'record_id':'pending-ui','routing_key':'r','expected':'Absent','document':{'title':'Pending vector','body':'alpha'}}
            page.locator('#documentJson').fill(json.dumps(pending));page.locator('#submitDocuments').click()
            expect(page.locator('#documentStatus')).to_contain_text('Wrote 1 record(s)')
            cid=page.evaluate('INDEX_ID')
            assert call('GET',f'/api/v1/index/{cid}/doc/pending-ui/status')['pending_vector_fields']==['vector']
            patch={'record_id':'pending-ui','routing_key':'r','expected':{'Match':1},'vector_patch':{'vector':[2,0]}}
            page.locator('#documentJson').fill(json.dumps(patch));page.locator('#submitDocuments').click()
            expect(page.locator('#documentStatus')).to_contain_text('Wrote 1 record(s)')
            assert call('GET',f'/api/v1/index/{cid}/doc/pending-ui/status')['pending_vector_fields']==[]
            assert call('GET',f'/api/v1/index/{cid}/doc/pending-ui')['version']==1
            page.locator('#recordEnvelopeMode').uncheck()
            # Known rejection can release only the rejected/unattempted remainder.
            page.locator('#documentJson').fill(json.dumps({'body':'bad vector','vector':[1]}))
            page.locator('#submitDocuments').click();expect(page.locator('#discardRejectedWrites')).to_be_visible()
            page.reload(wait_until="domcontentloaded");page.locator('#writeDocuments').click();expect(page.locator('#discardRejectedWrites')).to_be_visible()
            page.locator('#discardRejectedWrites').click();expect(page.locator('#documentJson')).to_be_enabled()
            expect(page.locator('#documentStatus')).to_contain_text('Rejected and unsent requests discarded')
            page.locator('#backToSearch').click()
        # Observe without intercepting/retrying requests or widening deadlines.
        # Keep operation identities, never authorization headers or document bodies.
        recovery_events=[];recovery_started=time.monotonic()
        def recovery_event(kind, **fields):
            if len(recovery_events)<64:
                recovery_events.append({'seconds':round(time.monotonic()-recovery_started,3),'kind':kind,**fields})
        def recovery_request(request):
            if '/api/v1/service-' in request.url:
                body=request.post_data_json or {}
                recovery_event('request',path=request.url.split('/api/v1/',1)[-1],operation_id=body.get('operation_id'))
        def recovery_response(response):
            if '/api/v1/service-' in response.url:
                recovery_event('response',path=response.url.split('/api/v1/',1)[-1],status=response.status)
        def recovery_failed(request):
            if '/api/v1/service-' in request.url:
                recovery_event('requestfailed',failure=request.failure)
        page.on('request',recovery_request);page.on('response',recovery_response);page.on('requestfailed',recovery_failed)
        page.on('console',lambda message:recovery_event('console',level=message.type,text=message.text[:512]))
        try:
            page.locator('#serviceRecovery summary').click()
            page.locator('#backupService').click()
            expect(page.locator('#serviceRecoveryStatus')).to_contain_text('Service backup created',timeout=30000)
            page.locator('#restoreService').click()
            expect(page.locator('#serviceRecoveryStatus')).to_contain_text('Restored to new stopped root',timeout=30000)
        finally:
            recovery_event('final',state=page.evaluate("({status:document.querySelector('#serviceRecoveryStatus').textContent,disabled:document.querySelector('#restoreService').disabled,pending:JSON.parse(sessionStorage.getItem('seekpower.pendingServicerestore')||'null')})"),page_errors=errors)
            print('SERVICE RECOVERY TRACE: '+json.dumps(recovery_events),flush=True)
            if os.environ.get('ANYSEEK_RECOVERY_TRACE_FILE'):
                pathlib.Path(os.environ['ANYSEEK_RECOVERY_TRACE_FILE']).write_text(json.dumps(recovery_events))
        assert not errors,errors
        context.close();browser.close()
    print('PASS Chromium: existing setup/editor/search, stable collection IDs, response-loss retry after reload without duplicate write, lexical/vector/hybrid creation')

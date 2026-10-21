"""Real Chromium checks for the retained UI, through the default service API."""
import json
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
        assert saved['requests']==writes and 'key' not in saved
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
            assert retained['unresolved'] and retained['requests']==writes
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
        publish={'record_id':'ui-generation','routing_key':'ui-route','expected':{'Match':1},'generation':{'action':'publish','generation_id':'g1'}}
        page.locator('#documentJson').fill(json.dumps(publish));page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        page.locator('#backToSearch').click();page.locator('#searchInput').fill('generationui')
        expect(page.locator('#contentBox')).to_contain_text('Generation A');expect(page.locator('#contentBox')).to_contain_text('Generation B')
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
        # Same-service takeover through real UI; response loss keeps the exact intent.
        from record_service_smoke import operation
        archive=call('POST',f'/api/v1/index/{collection}/backup',{'operation_id':operation()})
        final_write={'operation_id':operation(),'record_id':'after-ui-backup','routing_key':'ui','expected':'Absent','document':{'body':'retained final UI write'}}
        final_receipt=call('POST',f'/api/v1/index/{collection}/doc',final_write)['receipt']
        page.locator('#manageIndexes').click()
        page.locator('#takeoverRecovery summary').click()
        page.locator('#takeoverSource').fill(collection)
        page.locator('#takeoverBackup').fill(archive['backup_id'])
        page.locator('#takeoverTarget').fill('browser-takeover')
        takeover_requests=[]
        def lose_takeover(route):
            takeover_requests.append(route.request.post_data_json)
            reply=route.fetch();assert reply.status==200
            route.abort('failed')
        page.route('**/api/v1/takeovers',lose_takeover,times=1)
        page.locator('#submitTakeover').click()
        expect(page.locator('#takeoverStatus')).to_contain_text('Retry preserves')
        page.reload(wait_until='domcontentloaded');page.locator('#manageIndexes').click()
        page.locator('#takeoverRecovery summary').click()
        expect(page.locator('#takeoverSource')).to_be_disabled()
        assert page.evaluate("JSON.parse(sessionStorage.getItem('seekpower.pendingTakeover'))")==takeover_requests[0]
        page.locator('#submitTakeover').click()
        expect(page.locator('#takeoverStatus')).to_contain_text('Recovery active: browser-takeover')
        assert call('POST','/api/v1/index/browser-takeover/doc',final_write)['receipt']==final_receipt
        call('GET',f'/api/v1/index/{collection}',status=403)
        print('PASS Chromium: takeover final writes, original closed, exact retry after lost response and reload')
        page.locator('#serviceRecovery summary').click()
        page.locator('#backupService').click()
        expect(page.locator('#serviceRecoveryStatus')).to_contain_text('Service backup created',timeout=30000)
        page.locator('#restoreService').click()
        expect(page.locator('#serviceRecoveryStatus')).to_contain_text('Restored to new stopped root',timeout=30000)
        assert not errors,errors
        context.close();browser.close()
    print('PASS Chromium: existing setup/editor/search, stable collection IDs, response-loss retry after reload without duplicate write, lexical/vector/hybrid creation')

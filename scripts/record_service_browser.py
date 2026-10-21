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
        page.locator('details').first.locator('summary').click()
        schema=[{'field':'body','field_type':'Text','store':True,'boost':0}]
        page.locator('#indexSchema').fill(json.dumps(schema))
        page.locator('#createIndexForm button[type="submit"]').click()
        expect(page.locator('#discardRejectedCreation')).to_be_visible()
        page.reload();page.locator('#manageIndexes').click()
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
        page.reload();page.locator('#manageIndexes').click()
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
        page.reload();expect(page.locator('#indexName')).to_have_text('browser-records')
        page.locator('#writeDocuments').click();expect(page.locator('#retryDocumentCommit')).to_be_visible()
        page.locator('#retryDocumentCommit').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        assert page.evaluate("sessionStorage.getItem('seekpower.pendingWrites')") is None
        assert call('GET',f'/api/v1/index/{collection}')['indexed_doc_count']==1
        page.locator('#backToSearch').click()
        page.locator('#searchInput').fill('browser alpha')
        expect(page.locator('#contentBox')).to_contain_text('Browser title')
        page.reload();expect(page.locator('#indexName')).to_have_text('browser-records')
        # Keep existing Vector/Hybrid setup, with explicit model/space identity.
        for mode in ['Vector','Hybrid']:
            page.locator('#manageIndexes').click();expect(page.locator('#newIndexName')).to_be_enabled()
            page.locator('#indexMode').select_option(mode);page.locator('#newIndexName').fill('browser-'+mode)
            page.locator('#vectorDimensions').fill('2');page.locator('#embeddingSpaceId').fill('browser-model-v1')
            page.locator('#createIndexForm button[type="submit"]').click();expect(page.locator('#indexName')).to_have_text('browser-'+mode)
            page.locator('#writeDocuments').click();page.locator('#documentJson').fill(json.dumps({'title':mode,'body':'alpha','vector':[2,0]}))
            page.locator('#submitDocuments').click();expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
            # Known rejection can release only the rejected/unattempted remainder.
            page.locator('#documentJson').fill(json.dumps({'body':'bad vector','vector':[1]}))
            page.locator('#submitDocuments').click();expect(page.locator('#discardRejectedWrites')).to_be_visible()
            page.reload();page.locator('#writeDocuments').click();expect(page.locator('#discardRejectedWrites')).to_be_visible()
            page.locator('#discardRejectedWrites').click();expect(page.locator('#documentJson')).to_be_enabled()
            expect(page.locator('#documentStatus')).to_contain_text('Rejected and unsent requests discarded')
            page.locator('#backToSearch').click()
        assert not errors,errors
        context.close();browser.close()
    print('PASS Chromium: existing setup/editor/search, stable collection IDs, response-loss retry after reload without duplicate write, lexical/vector/hybrid creation')

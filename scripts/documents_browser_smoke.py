"""JSON write, commit and immediate search through the actual embedded UI/API."""
import json
from playwright.sync_api import expect


def exercise_documents(page, mode):
    page.locator('#writeDocuments').click()
    expect(page.locator('#documentPanel')).to_be_visible()
    expect(page.locator('#documentIndex')).to_contain_text('ui-' + mode)
    posts = []

    def record_post(request):
        if request.method == 'POST' and request.url.endswith('/doc'):
            posts.append(request)
    page.on('request', record_post)
    page.locator('#documentJson').fill('[null]')
    page.locator('#submitDocuments').click()
    expect(page.locator('#documentError')).to_contain_text('JSON object')
    assert not posts
    marker = 'uimarker'
    records = [{'title': marker, 'body': 'first <b>literal</b>', 'url': ''},
               {'title': marker, 'body': 'second record', 'url': ''}]
    if mode != 'Lexical':
        records[0]['vector'] = [1, 0, 0]
        records[1]['vector'] = [0.9, 0.1, 0]
        page.locator('#documentJson').fill(json.dumps({'body': marker, 'vector': [1]}))
        with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/doc')) as rejected:
            page.locator('#submitDocuments').click()
        assert rejected.value.status == 400
        expect(page.locator('#documentError')).to_contain_text('expected 3 values')
        expect(page.locator('#submitDocuments')).to_be_enabled()

    page.locator('#documentJson').fill(json.dumps(records))
    page.locator('#submitDocuments').click()
    expect(page.locator('#documentStatus')).to_contain_text('Wrote 2 document(s) and committed')
    expect(page.locator('#documentStatus')).to_contain_text('contains 2 document(s)')
    expect(page.locator('#documentJson')).to_have_value('')
    # A single object from a local JSON file uses exactly the same REST writer.
    content = json.dumps(records[0])
    page.locator('#documentFile').set_input_files({
        'name': 'record.json', 'mimeType': 'application/json', 'buffer': content.encode()})
    expect(page.locator('#documentJson')).to_have_value(content)
    page.locator('#submitDocuments').click()
    expect(page.locator('#documentStatus')).to_contain_text('contains 3 document(s)')
    expected_count = 3

    if mode == 'Lexical':
        # A rejected commit must be retried without posting duplicate records.
        def reject_commit(route):
            if route.request.method == 'PATCH':
                route.fulfill(status=503, body='temporary commit failure')
            else:
                route.continue_()
        page.route('**/api/v1/index/0', reject_commit)
        page.locator('#documentJson').fill(content)
        page.locator('#submitDocuments').click()
        expect(page.locator('#documentError')).to_contain_text('Records were accepted')
        expect(page.locator('#submitDocuments')).to_be_disabled()
        expect(page.locator('#retryDocumentCommit')).to_be_visible()
        before_retry = len(posts)
        page.unroute('**/api/v1/index/0', reject_commit)
        page.locator('#retryDocumentCommit').click()
        expect(page.locator('#documentStatus')).to_contain_text('contains 4 document(s)')
        assert len(posts) == before_retry, 'commit retry posted duplicate documents'
        expected_count = 4

    page.locator('#backToSearch').click()
    expect(page.locator('#indexedDocs')).to_have_text(str(expected_count))
    page.locator('#searchInput').fill(marker)
    if mode != 'Lexical':
        page.locator('label[data-index="0"]' if mode == 'Vector' else 'label[data-index="1"]').click()
        page.locator('#queryVector').fill('[1,0,0]')
        page.locator('#vectorSearch').click()
    expect(page.locator('#contentBox .resultEntry')).to_have_count(expected_count)
    expect(page.locator('#contentBox')).to_contain_text('first <b>literal</b>')
    page.remove_listener('request', record_post)
    print(f'PASS Chromium {mode} JSON batch/file write, commit, count and search')

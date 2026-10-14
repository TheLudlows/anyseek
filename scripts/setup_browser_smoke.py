"""Real-browser onboarding regression, called by the existing HTTP smoke runner."""
import json
from playwright.sync_api import expect


def exercise_setup(browser, base, initial_key, master):
    context = browser.new_context(viewport={'width': 1100, 'height': 900})
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(base)
    # The existing demo scope may be available; enter setup explicitly.
    page.locator('#manageIndexes').click()
    expect(page.locator('#setupKey')).to_be_enabled()
    page.locator('#setupKey').fill(master)
    page.locator('#connectForm button[type="submit"]').click()
    expect(page.locator('#setupError')).to_contain_text('Invalid API key')
    expect(page.locator('#indexManager')).to_be_hidden()
    page.locator('#setupKey').fill(initial_key)
    page.locator('#connectForm button[type="submit"]').click()
    expect(page.locator('#indexListStatus')).to_contain_text('No indexes yet')
    expect(page.locator('#openIndex')).to_be_disabled()

    # Client validation must not issue a create request.
    creates = []
    page.on('request', lambda request: creates.append(request.post_data_json)
            if request.method == 'POST' and request.url.endswith('/api/v1/index') else None)
    page.locator('#newIndexName').fill('invalid')
    page.locator('#createIndexForm summary').click()
    page.locator('#indexSchema').fill('[]')
    page.locator('#createIndexForm button').click()
    expect(page.locator('#setupError')).to_contain_text('nonempty JSON array')
    assert not creates

    for mode in ('Lexical', 'Vector', 'Hybrid'):
        if mode != 'Lexical':
            page.locator('#manageIndexes').click()
            expect(page.locator('#newIndexName')).to_be_enabled()
        # Trigger preset even for the first iteration, after malformed schema.
        page.locator('#indexMode').select_option('Hybrid' if mode == 'Lexical' else 'Lexical')
        page.locator('#indexMode').select_option(mode)
        page.locator('#newIndexName').fill('ui-' + mode)
        if mode != 'Lexical':
            page.locator('#vectorDimensions').fill('3')
        with page.expect_response(lambda response: response.request.method == 'POST'
                                  and response.url.endswith('/api/v1/index')) as response:
            page.locator('#createIndexForm button').click()
        assert response.value.status == 200, response.value.text()
        expect(page.locator('#indexName')).to_have_text('ui-' + mode)
        expect(page.locator('#welcome')).to_be_hidden()
        assert 'api_key' not in page.url
        payload = creates[-1]
        if mode != 'Lexical':
            assert payload['inference']['External']['dimensions'] == 3
            assert any(field.get('index_vector') for field in payload['schema'])
        else:
            assert 'inference' not in payload
        from documents_browser_smoke import exercise_documents
        exercise_documents(page, mode)

    page.reload()
    expect(page.locator('#indexName')).to_have_text('ui-Hybrid')
    page.locator('#manageIndexes').click()
    expect(page.locator('#indexList option')).to_have_count(3)
    page.locator('#indexList').select_option('0')
    page.locator('#openIndex').click()
    expect(page.locator('#indexName')).to_have_text('ui-Lexical')
    # A server validation error preserves the form and permits retry.
    page.locator('#manageIndexes').click()
    expect(page.locator('#newIndexName')).to_be_enabled()
    page.locator('#indexMode').select_option('Lexical')
    page.locator('#newIndexName').fill('server-rejected')
    page.locator('#createIndexForm details').evaluate("element => element.open = true")
    page.locator('#indexSchema').fill(json.dumps([
        {'field': 'body', 'field_type': 'InvalidType', 'store': True, 'index_lexical': True}]))
    page.locator('#createIndexForm button').click()
    expect(page.locator('#setupError')).not_to_be_empty()
    expect(page.locator('#newIndexName')).to_have_value('server-rejected')
    expect(page.locator('#createIndexForm button')).to_be_enabled()
    page.screenshot(path='/tmp/seekpower-setup-desktop.png', full_page=True)
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'mobile setup overflows'
    page.screenshot(path='/tmp/seekpower-setup-mobile.png', full_page=True)

    page.locator('#forgetKey').click()
    expect(page.locator('#setupKey')).to_have_value('')
    expect(page.locator('#indexManager')).to_be_hidden()
    page.reload()
    expect(page.locator('#welcome')).to_be_visible()
    expect(page.locator('#setupKey')).to_have_value('')
    assert not errors, errors
    context.close()
    print('PASS Chromium API-key connection, scope, lexical/vector/hybrid creation, validation, switch, reload and forget')

import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import ts from 'typescript';
import { getLanguage, setLanguage, subscribeLanguage, t, localizeNotice } from '../src/i18n.ts';
import { english } from '../src/messages.ts';
afterEach(() => setLanguage('zh'));
test('language preference persists and placeholders preserve user text', () => {
  const data = new Map<string,string>();
  Object.defineProperty(globalThis, 'localStorage', { configurable:true, value: { getItem:(k:string)=>data.get(k), setItem:(k:string,v:string)=>data.set(k,v) } });
  let count=0; const off=subscribeLanguage(()=>count++);
  setLanguage('en'); assert.equal(getLanguage(),'en'); assert.equal(data.get('anyseek.language'),'en');
  assert.equal(t('集合'),'Collections'); assert.equal(t('集合 {0} 已删除。','我的文档{1}'),'Collection 我的文档{1} deleted.');
  assert.equal(t('unknown'),'unknown'); assert.equal(count,1); off();
});
test('storage failure permits switching and saved notices follow language',()=>{
  Object.defineProperty(globalThis,'localStorage',{configurable:true,value:{setItem(){throw new Error('blocked');}}});
  setLanguage('en'); assert.equal(localizeNotice('已确认提交 12 条，索引正在异步更新。'),'Confirmed submission of 12 records. Indexes are updating asynchronously.');
  setLanguage('zh'); assert.equal(localizeNotice('Collection example deleted.'),'集合 example 已删除。');
});
test('all UI translation keys exist and keep their placeholders',()=>{
  const root=new URL('../src/',import.meta.url);
  for(const file of fs.readdirSync(root).filter(f=>/\.tsx?$/.test(f)&&!['messages.ts','i18n.ts'].includes(f))){
    const source=ts.createSourceFile(file,fs.readFileSync(new URL(file,root),'utf8'),ts.ScriptTarget.Latest,true);
    function walk(n:ts.Node){
      if(ts.isCallExpression(n)&&n.expression.getText(source)==='t'&&n.arguments[0]&&ts.isStringLiteral(n.arguments[0])){
        const key=n.arguments[0].text;
        assert.ok(key in english,`${file}: ${key}`);
        assert.deepEqual((english[key].match(/\{\d+\}/g)||[]).sort(),(key.match(/\{\d+\}/g)||[]).sort(),key);
      }
      ts.forEachChild(n,walk);
    } walk(source);
  }
});

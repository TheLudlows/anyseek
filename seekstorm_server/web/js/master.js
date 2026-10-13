var API_KEY="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";var INDEX_ID=0;var TITLE_FIELD="title";var TEXT_FIELD="body";var URL_FIELD="url";var IMAGE_FIELD="image";var DATE_FIELD="date";var QUERY_URL=`api/v1/index/${INDEX_ID}/query`;var STATUS_URL=`api/v1/index/${INDEX_ID}`;var IS_DATE_FIELD=false;var status_loaded=false;let query="";let maxPages=-1;let currPage=1;let currSort='score';let search_mode="Lexical";function debounce(func,wait,immediate){var timeout;return function(){var context=this,args=arguments;var later=function(){timeout=null;if(!immediate)func.apply(context,args);};var callNow=immediate&&!timeout;clearTimeout(timeout);timeout=setTimeout(later,wait);if(callNow)func.apply(context,args);};}
function moveSplitter(y,hide=false){const[l1,l2,l3,l4]=[$("#l1"),$("#l2"),$("#l3"),$("#l4")];l1.attr("y2",y-70);l2.attr("y1",y-70);l2.attr("y2",y-30);l3.attr("y1",y-30);l3.attr("y2",y+10);l4.attr("y1",y+10);if(hide)$("#splitter").hide();else $("#splitter").show();}
function updateTitle(useDefault=false){const title=query&&!useDefault?`${query}-SeekStorm Search`:"SeekStorm Search";if(document.title!=title){document.title=title;}}
let active=null;function setHeader(xhr){xhr.setRequestHeader('apikey',API_KEY);}
function loadResults(page,sort,ignoreHistory=false){
let queryVector;
$("#searchError").text("");
if(search_mode!=="Lexical"){
try {
const raw=document.getElementById("queryVector").value.trim();
if(!raw)throw new Error("Vector and Hybrid search require a query vector.");
queryVector=JSON.parse(raw);
if(!Array.isArray(queryVector)||queryVector.length===0||!queryVector.every(v=>typeof v==="number"&&Number.isFinite(v)))throw new Error("Enter a nonempty JSON array of finite numbers.");
}catch(error){$("#searchError").text(error.message);$("#contentBox").empty();return;}
}
updateTitle();active=null;if((currPage!=page||currSort!=sort)&&!ignoreHistory){var newURL=updateURLParameter(window.location.href,"q",query);newURL=updateURLParameter(newURL,"page",page);newURL=updateURLParameter(newURL,"sort",sort);window.history.pushState("","",newURL);}
currPage=page;currSort=sort;$('#sort button').removeClass('active');$('#sort button').filter(function(){return this.id===sort;}).addClass('active');const contentBox=$("#contentBox");const prevUrl=$("#prevUrl");const prevTitle=$("#prevTitle");const prevText=$("#prevText");const prevText2=$("#prevText2");moveSplitter(-50,true);prevUrl.hide();prevTitle.text("");prevText2.text("");prevText.hide();if(!status_loaded)showStatus(false);let facetFilter=[];
if(IS_DATE_FIELD){
const from=document.getElementById('dateFrom').value;
const to=document.getElementById('dateTo').value;
if(from||to){
const start=from?Date.parse(from+'T00:00:00Z')/1000:Number.MIN_SAFE_INTEGER;
const end=to?Date.parse(to+'T00:00:00Z')/1000+86400:Number.MAX_SAFE_INTEGER;
if(!Number.isFinite(start)||!Number.isFinite(end)||start>=end){$('#searchError').text('Enter a valid date range.');return;}
facetFilter=[{Timestamp:{field:DATE_FIELD,filter:{start:start,end:end}}}];
}
}
$.ajax({url:QUERY_URL,contentType:"application/json",data:JSON.stringify({query:query,query_vector:queryVector,enable_empty_query:true,offset:(page-1)*10,length:10,realtime:true,result_sort:sort=='score'?[]:sort=='new'?[{field:DATE_FIELD,order:"Descending"}]:[{field:DATE_FIELD,order:"Ascending"}],facet_filter:facetFilter,search_mode:search_mode}),traditional:true,cache:false,type:"POST",dataType:'json',success:function(data){contentBox.empty();
data.results.forEach(element=>{
const linkText=valid(element[URL_FIELD]);
const linkUrl=httpUrl(linkText);
const dateValue=element[DATE_FIELD];
const dateText=dateValue!=null?" - "+(new Date(dateValue*1000)).toLocaleDateString("en-US",{year:'numeric',month:'numeric',day:'numeric'}):'';
const titleText=valid(element[TITLE_FIELD])+dateText;
const bodyText=valid(element[TEXT_FIELD]);
const imageData=valid(element[IMAGE_FIELD]);
const imageUrl=httpUrl(valid(element.iurl));
const newEntry=$('<div class="resultEntry col-xs-12"><div class="row"><div class="record"><h1 style="padding-bottom:4px;"><a></a></h1><p class="recordText"></p><p class="directLink"><a class="url"></a></p></div></div></div>');
newEntry.find('.record').addClass(imageUrl?'col-xs-10':'col-xs-12');
newEntry.find('h1 a').text(titleText);
newEntry.find('.recordText').text(bodyText);
newEntry.find('a.url').text(linkText);
if(linkUrl)newEntry.find('a').attr('href',linkUrl);
if(imageUrl)$('<div class="col-xs-2 imgPrev"></div>').css('background-image','url('+JSON.stringify(imageUrl)+')').prependTo(newEntry.find('.row'));
if(imageData.startsWith('data:image')){const image=new Image();image.src=imageData;image.height=100;newEntry.prepend(image);}
contentBox.append(newEntry);
newEntry.on('mouseenter',()=>{
moveSplitter(newEntry[0].getBoundingClientRect().y);
prevTitle.text(titleText);prevUrl.hide();prevText.hide();prevText2.text('');
if(linkUrl){prevUrl.text(linkText).show();prevText.attr('src',linkUrl).show();}
else{prevText2.text(bodyText);}
});
});
setPage(page,data.count,data.count_total,data.time);},error:function(xhr){$("#contentBox").empty();$("#searchError").text(xhr.responseText||"Search request failed.");},beforeSend:setHeader});}
function valid(value){return value==null?'':typeof value==='object'?JSON.stringify(value):String(value);}
function httpUrl(value){try{const url=new URL(value);return ['http:','https:'].includes(url.protocol)?url.href:null;}catch{return null;}}
function showStatus(async=true){
$.ajax({url:STATUS_URL,traditional:true,cache:false,type:'GET',async:async,dataType:'json',
success:function(data){
$('#welcome').hide();$('#version').text(data.version);$('#indexName').text(data.name);$('#indexedDocs').text(data.indexed_doc_count.toLocaleString());
const schema=new Map(Object.entries(data.schema).sort((a,b)=>a[1].field_id-b[1].field_id));
const textFields=[...schema].filter(([key,value])=>value.stored&&value.field_type!=='Binary'&&value.field_type!=='Timestamp'&&value.field_type!=='U32').map(([key])=>key);
for(const target of ['title','body','url']){
if(target==='title'&&!data.schema[TITLE_FIELD])TITLE_FIELD=textFields.shift()||TITLE_FIELD;
if(target==='body'&&!data.schema[TEXT_FIELD])TEXT_FIELD=textFields.find(key=>key!==TITLE_FIELD)||TEXT_FIELD;
if(target==='url'&&!data.schema[URL_FIELD])URL_FIELD=textFields.find(key=>key!==TITLE_FIELD&&key!==TEXT_FIELD)||URL_FIELD;
}
const dateFields=[...schema].filter(([key,value])=>value.field_type==='Timestamp'&&value.facet).map(([key])=>key);
IS_DATE_FIELD=dateFields.length>0;
if(IS_DATE_FIELD&&!dateFields.includes(DATE_FIELD))DATE_FIELD=dateFields[0];
for(const [key,value] of schema){if(value.field_type==='Binary'&&value.stored&&!data.schema[IMAGE_FIELD]){IMAGE_FIELD=key;break;}}
document.getElementById('dateControls').hidden=!IS_DATE_FIELD;
status_loaded=true;
},error:function(){$('#landingSearch').hide();$('#welcome').css('display','flex');},beforeSend:setHeader});
}
function setPage(page,results,count_total,statsTime){maxPages=Math.ceil(count_total/10);let vars=$("#footer").children();const[from,to,count,stats]=[$("#from"),$("#to"),$("#count"),$("#stats")];from.text(((page-1)*10+1).toLocaleString());to.text(Math.min(page*10,count_total).toLocaleString());count.text(count_total.toLocaleString());stats.attr("title",`${count_total.toLocaleString()} results in ${(statsTime/1000_000).toLocaleString(undefined,{minimumFractionDigits:0,maximumFractionDigits:3})}ms`);for(const element of vars){const elem=$(element);elem.removeClass("active");elem.show();}
if(page==1)$(vars[0]).hide();else $(vars[0]).show();if(page==maxPages)$(vars[11]).hide();else $(vars[11]).show();if(page>5){for(let i=0;i<10;i++){const element=$(vars[i+1]);element.text(page-5+i);if(i==5){element.addClass("active");}
if(page-5+i>maxPages){element.hide();}}}else{for(let i=0;i<10;i++){const element=$(vars[i+1]);element.text(i+1);if(i==page-1){element.addClass("active");}
if(i>=maxPages){element.hide();}}}}
$(function(){
$('#dateApply').on('click',()=>loadResults(1,currSort));
$('#dateClear').on('click',()=>{$('#dateFrom,#dateTo').val('');loadResults(1,currSort);});
$('#sort button').on('click',function(){const sort=this.id;$('#sort button').removeClass('active');$(this).addClass('active');loadResults(1,sort);});
$("#vectorSearch").on("click",()=>loadResults(1,"score"));$("#searchInput").on("input",function(e){e.preventDefault();$("#landingSearch").hide();$("#landingMain").show();$("#searchMain").val($("#searchInput").val()).trigger("input");$("#searchMain").focus();$("#searchInput").val("");});$("#footer, #topBar").on("mouseenter",function(){const prevUrl=$("#prevUrl");const prevTitle=$("#prevTitle");const prevText=$("#prevText");const prevText2=$("#prevText2");moveSplitter(-50,true);prevUrl.hide();prevTitle.text("");prevText2.text("");prevText.hide();});$("#topLogo").on("click",function(e){e.preventDefault();$("#landingSearch").show();$("#searchInput").focus();$("#landingMain").hide();updateTitle(true);window.history.pushState("","",window.location.href.split("?")[0]);});const mainSearch=$("#searchMain");mainSearch.on("input",debounce(()=>{query=mainSearch.val();loadResults(1,'score');},50));mainSearch.on("input",debounce(()=>{var newURL=updateURLParameter(window.location.href,"q",query);newURL=updateURLParameter(newURL,"page",1);window.history.pushState("","",newURL);},700));$("#footer div").click(event=>{let target=$(event.target);if(!target.hasClass("action")){loadResults(target.text(),currSort);}else{const cPage=parseInt($("#footer div.active")[0].innerText);if(target.attr("next")){loadResults(cPage+1,currSort);}else{loadResults(cPage-1,currSort);}}});window.addEventListener("popstate",function(e){checkUrl();});checkUrl();$('.switch-label').on('click',function(){const index=$(this).data('index');const value=$(this).find('input').val();$('.switch-label').removeClass('active');$(this).addClass('active');const position=index*33.33;$('.switch-selector').css('left',`calc(${position}% + 4px)`);$(this).find('input').prop('checked',true);switch(value){case"Vector":search_mode={Vector:{similarity_threshold:null,ann_mode:{"Nprobe":55}}};break;case"Lexical":search_mode="Lexical";break;default:search_mode={Hybrid:{similarity_threshold:null,ann_mode:{"Nprobe":55}}};}
document.getElementById("vectorControls").hidden=search_mode==="Lexical";loadResults(1,'score');});showStatus();});function checkUrl(){const queryStrings=parse_query_string(window.location.search.substring(1));if(queryStrings.api_key){API_KEY=queryStrings.api_key;}
if((queryStrings.index_id)&&!isNaN(queryStrings.index_id)){STATUS_URL=`api/v1/index/${queryStrings.index_id}`;QUERY_URL=`api/v1/index/${queryStrings.index_id}/query`;}
if(queryStrings.q){query=queryStrings.q;let loadPage=1;if(queryStrings.page)loadPage=parseInt(queryStrings.page);loadResults(loadPage,'score',true);$("#searchMain").val(query);$("#landingSearch").hide();$("#landingMain").show();}else{$("#landingSearch").show();$("#searchInput").focus();$("#landingMain").hide();}}
$(document).keydown(function(e){if($("#queryVector").is(":focus")||$("#searchMain").is(":focus")||$("#footer div.active")[0]===undefined)
return;const cPage=parseInt($("#footer div.active")[0].innerText);switch(e.which){case 38:if(cPage>1)loadResults(cPage-1,'score');break;case 40:if(cPage<maxPages)loadResults(cPage+1,'score');break;default:return;}
e.preventDefault();});document.addEventListener("touchstart",handleTouchStart,false);document.addEventListener("touchmove",handleTouchMove,false);var xDown=null;var yDown=null;function getTouches(evt){return(evt.touches||evt.originalEvent.touches);}
function handleTouchStart(evt){const firstTouch=getTouches(evt)[0];xDown=firstTouch.clientX;yDown=firstTouch.clientY;}
function handleTouchMove(evt){if(!xDown||!yDown){return;}
var xUp=evt.touches[0].clientX;var yUp=evt.touches[0].clientY;var xDiff=xDown-xUp;var yDiff=yDown-yUp;if(Math.abs(xDiff)>Math.abs(yDiff)){const cPage=parseInt($("#footer div.active")[0].innerText);if(xDiff>0){if(cPage<maxPages)loadResults(cPage+1,'score');}else{if(cPage>1)loadResults(cPage-1,'score');}}else{if(yDiff>0){}else{}}
xDown=null;yDown=null;}
function updateURLParameter(url,param,paramVal){var TheAnchor=null;var newAdditionalURL="";var tempArray=url.split("?");var baseURL=tempArray[0];var additionalURL=tempArray[1];var temp="";if(additionalURL){var tmpAnchor=additionalURL.split("#");var TheParams=tmpAnchor[0];TheAnchor=tmpAnchor[1];if(TheAnchor)additionalURL=TheParams;tempArray=additionalURL.split("&");for(var i=0;i<tempArray.length;i++){if(tempArray[i].split("=")[0]!=param){newAdditionalURL+=temp+tempArray[i];temp="&";}}}else{var tmpAnchor=baseURL.split("#");var TheParams=tmpAnchor[0];TheAnchor=tmpAnchor[1];if(TheParams)baseURL=TheParams;}
if(TheAnchor)paramVal+="#"+TheAnchor;var rows_txt=temp+""+param+"="+paramVal;return baseURL+"?"+newAdditionalURL+rows_txt;}
function parse_query_string(query){var vars=query.split("&");var query_string={};for(var i=0;i<vars.length;i++){var pair=vars[i].split("=");if((pair.length==3)&&(pair[2]==""))pair[1]=pair[1]+"=";var key=decodeURIComponent(pair[0]);var value=decodeURIComponent(pair[1]);if(typeof query_string[key]==="undefined"){query_string[key]=decodeURIComponent(value);}else if(typeof query_string[key]==="string"){var arr=[query_string[key],decodeURIComponent(value)];query_string[key]=arr;}else{query_string[key].push(decodeURIComponent(value));}}
return query_string;}

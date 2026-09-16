const $ = id => document.getElementById(id);
const nf = new Intl.NumberFormat('ru-RU');
const compact = new Intl.NumberFormat('ru-RU', {notation:'compact', maximumFractionDigits:1});
const number = n => n == null ? '—' : nf.format(Math.round(n));
const short = n => n == null ? '—' : n < 100000 ? number(n) : compact.format(n);
const percent = n => n == null ? '—' : n.toLocaleString('ru-RU',{maximumFractionDigits:1})+'%';
const date = (t, clock=true) => t == null ? 'нет данных' : new Date(t).toLocaleString('ru-RU',{day:'2-digit',month:'short',...(clock?{hour:'2-digit',minute:'2-digit'}:{})});
const esc = value => String(value ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const colors = ['#2868bf','#b77f20','#8060be','#318c79','#c96360','#557989','#778634'];
const color = name => (document.documentElement.dataset.theme==='dark'?['#75abed','#e5b663','#b79be7','#81b9a3','#ef9892','#88afc0','#bec887']:colors)[[...String(name)].reduce((s,c)=>(s*31+c.charCodeAt(0))>>>0,0)%colors.length];
const names = {unknown:'Неизвестно',fast:'Fast',standard:'Стандарт',flex:'Flex',auto:'Auto'};
const display = v => names[v] || v;
const money = n => n == null ? '—' : new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:n>0&&n<.01?6:2}).format(n);
const cost = t => t.api_cost_usd!=null ? '≈ '+money(t.api_cost_usd) : t.api_cost_known_usd!=null ? '≥ '+money(t.api_cost_known_usd) : '—';
const dimensions = ['machine','model','effort','tier'];
const defaults = {machine:'Все машины',model:'Все модели',effort:'Любой effort',tier:'Все режимы'};
let state=null, schema=null, requestId=0, controller=null, selectedSession=null;
const hiddenSeries=new Set();
function localDay(d){return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;}
$('from-date').value=localDay(new Date(Date.now()-6*86400000));$('to-date').value=localDay(new Date());
function interval(){
  const now=Date.now(), choice=$('period').value;
  if (/^\d+$/.test(choice))return {start:now-Number(choice)*3600000,end:now};
  const today=new Date();today.setHours(0,0,0,0);
  if(choice==='today')return {start:+today,end:now};
  if(choice==='yesterday'){const d=new Date(today);d.setDate(d.getDate()-1);return {start:+d,end:+today};}
  const start=+new Date($('from-date').value+'T00:00:00'), next=new Date($('to-date').value+'T00:00:00');next.setDate(next.getDate()+1);
  const end=Math.min(+next,now);
  if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start||end-start>31*86400000)throw Error('Выберите интервал до 31 дня; конечная дата должна быть не раньше начальной.');
  return {start,end};
}
function params(fixed=false){
  const range=fixed&&state?{start:state.period.start_ms,end:state.period.end_ms}:interval();
  const q=new URLSearchParams({start:String(range.start),end:String(range.end),timezone:Intl.DateTimeFormat().resolvedOptions().timeZone||'UTC'});
  for(const key of dimensions){const value=fixed&&state?state.filters[key]:$(key).value;if(value)q.set(key,value);}
  q.set('zero_output',fixed&&state?state.filters.zero_output:$('zero-output').checked?'include':'exclude');if(fixed&&state)q.set('timezone',state.filters.timezone||'UTC');return q;
}
function showError(message){$('error').textContent=message;$('error').hidden=!message;}
function age(ms){if(ms==null)return 'время получения не записано';const sec=Math.max(0,Math.floor(ms/1000));if(sec<60)return `${sec} с назад`;if(sec<3600)return `${Math.floor(sec/60)} мин назад`;if(sec<86400)return `${Math.floor(sec/3600)} ч назад`;return `${Math.floor(sec/86400)} дн назад`;}
function setMetric(id,value){$(id).textContent=short(value);$(id).title=number(value)+' токенов';}
function percentage(a,b){return a==null||!b?null:100*a/b;}
function activity(){
  $('activity').replaceChildren();
  const hidden=state.machine_activity.filter(m=>m.hidden);
  for(const m of state.machine_activity.filter(m=>!m.hidden)){
    const card=document.createElement('div');card.className='machine-card'+($('machine').value===m.machine?' selected':'');
    const b=document.createElement('button');b.className='machine-select';
    const total=state.breakdowns.machine.find(row=>row.key===m.machine);
    b.innerHTML=`<span class="machine-name">${esc(m.machine)}</span><span class="machine-signal"><i class="signal-dot ${m.status==='recent'?'recent':''}"></i>${m.status==='recent'?'Телеметрия '+age(m.age_ms):m.status==='quiet'?'Последний сигнал '+age(m.age_ms):'Последнее событие '+date(m.last_event_ms)}</span><span class="machine-cost">${total?`<strong>${cost(total)}</strong> · ${short(total.total_tokens)} токенов`:'Нет событий в выборке'}</span>`;
    const sync=document.createElement('span');sync.className='machine-signal';
    sync.textContent=m.journal?`${m.journal.age_ms==null?'Ожидается первый отчёт':'Сверка '+age(m.journal.age_ms)} · очередь в отчёте: ${number(m.journal.pending_events)} · сессий: ${number(m.journal.active_sessions)}${m.journal.mismatched_days?' · есть расхождения':''}${m.journal.last_error?' · '+m.journal.last_error:''}`:'OTLP · очередь и полнота не проверяются';
    b.append(sync);
    b.title=[m.host,m.client,m.version,'Цена и токены по текущим фильтрам'].filter(Boolean).join(' · ');b.onclick=()=>choose('machine',$('machine').value===m.machine?'':m.machine);
    const remove=document.createElement('button');remove.className='machine-remove';remove.textContent='×';remove.setAttribute('aria-label','Убрать карточку '+m.machine);remove.title='Убрать карточку. История сохранится.';remove.onclick=()=>visibility(m.machine,true,remove);
    card.append(b,remove);$('activity').append(card);
  }
  $('hidden-machines').hidden=!hidden.length;$('hidden-count').textContent=`Скрытые карточки · ${hidden.length}`;$('hidden-list').replaceChildren();
  for(const m of hidden){const b=document.createElement('button');b.className='button secondary';b.textContent='Вернуть '+m.machine;b.onclick=()=>visibility(m.machine,false,b);$('hidden-list').append(b);}
  if(!state.machine_activity.length)$('activity').textContent='Машины появятся после первого события Codex.';
  else if(state.machine_activity.length===hidden.length)$('activity').textContent='Все карточки скрыты. Их можно вернуть ниже.';
}
async function visibility(machine,hidden,button){
  button.disabled=true;
  try{const r=await fetch('/api/v1/machines/visibility',{method:'POST',headers:{'Content-Type':'application/json','X-Codex-Usage-Action':'visibility'},body:JSON.stringify({machine,hidden})});if(!r.ok)throw Error('Не удалось изменить карточку (HTTP '+r.status+').');await refresh();}
  catch(e){showError(e.message);button.disabled=false;}
}
function facets(){
  for(const key of dimensions){
    const selected=$(key).value, values=[...new Set([...(state.facets[key]||[]),...(selected?[selected]:[])])];
    $(key).replaceChildren();
    for(const [value,text] of [['',defaults[key]],...values.map(v=>[v,display(v)])]){const option=document.createElement('option');option.value=value;option.textContent=text;$(key).append(option);}
    $(key).value=selected;
  }
}
function render(){
  if(!state)return;
  facets();activity();const t=state.totals;
  setMetric('total',t.total_tokens);setMetric('uncached',t.uncached_input_tokens);setMetric('cache',t.cached_input_tokens);setMetric('reasoning',t.reasoning_output_tokens);setMetric('visible',t.visible_output_tokens);
  const delta=state.comparison.change_pct.total_tokens;
  $('total-change').textContent=delta==null?'Нет базы для сравнения':`${delta>0?'+':''}${percent(delta)} к предыдущему интервалу`;
  $('uncached-note').textContent=`${percent(percentage(t.uncached_input_tokens,t.total_tokens))} от всех токенов`;
  $('cache-note').textContent=`${percent(t.cache_total_pct)} от всех · ${percent(t.cache_input_pct)} от входа`;
  $('reasoning-note').textContent=`${percent(t.reasoning_total_pct)} от всех · ${percent(t.reasoning_output_pct)} от выхода`;
  $('visible-note').textContent=`${percent(percentage(t.visible_output_tokens,t.total_tokens))} от всех токенов`;
  $('sample-count').textContent=`${number(t.events)} событий · ${number(t.sessions)} сессий`;
  $('average').textContent=`В среднем ${short(t.average_tokens)} токенов / событие`;
  $('input-total').textContent=`Вход с кэшем: ${short(t.input_tokens)}`;
  $('coverage').textContent=`Effort известен: ${percent(state.coverage.effort_known_pct)} событий · скорость: ${percent(state.coverage.tier_known_pct)}`;
  $('range-label').textContent=`${date(state.period.start_ms)} — ${date(state.period.end_ms)}`;
  $('data-start').textContent=state.coverage.first_observed_ms?'Первые данные: '+date(state.coverage.first_observed_ms,false):'Ожидаем первые данные';
  $('comparison-note').textContent=`Сравнение с ${date(state.period.previous_start_ms)} — ${date(state.period.previous_end_ms)}. `+(state.coverage.previous_starts_before_first_observation?'Предыдущий интервал начинается до первых наблюдений; сравнение неполное.':'Наличие событий не гарантирует отсутствие пропусков.');
  renderCost();renderRatios();renderRankings();renderSessions();drawChart();
}
function renderCost(){
  const t=state.totals;$('api-cost').textContent=cost(t);
  for(const [id,key] of [['cost-input','api_ordinary_input_usd'],['cost-read','api_cached_input_usd'],['cost-write','api_cache_write_usd'],['cost-output','api_output_usd']])$(id).textContent=t[key]!=null?money(t[key]):t.api_cost_breakdown_known_usd[key]!=null?'≥ '+money(t.api_cost_breakdown_known_usd[key]):'—';
  $('cost-note').textContent=`Тарифы на ${state.pricing.as_of}. Рассчитано ${number(t.api_priced_events)} из ${number(t.events)} событий.`+(t.api_assumed_tier_events?` Для ${number(t.api_assumed_tier_events)} событий скорость неизвестна или Auto — в цене принят Standard.`:' Известные режимы учтены.')+(t.api_unpriced_events?' ≥ — рассчитанная часть; у остальных нет тарифа или нужных счётчиков.':'')+' Эквивалент API; не списание подписки.';
}
function renderRatios(){
  const t=state.totals, rows=[['Кэш от всех токенов','кэш ÷ (вход + выход)',t.cache_total_pct],['Кэш от входных','кэш ÷ вход',t.cache_input_pct],['Reasoning от всех токенов','reasoning ÷ (вход + выход)',t.reasoning_total_pct],['Reasoning от выходных','reasoning ÷ выход',t.reasoning_output_pct],['Вход без кэша от всех','(вход − кэш) ÷ (вход + выход)',percentage(t.uncached_input_tokens,t.total_tokens)]];
  $('ratios').innerHTML=rows.map(([name,base,value])=>`<div class="ratio-row"><div><div class="ratio-name">${name}</div><div class="ratio-base">${base}</div></div><div class="ratio-value">${percent(value)}</div></div>`).join('');
}
function renderRankings(){
  const key=$('group-by').value,metric=$('ranking-metric').value,isCost=metric==='api_cost_known_usd',rows=[...(state.breakdowns[key]||[])].sort((a,b)=>(b[metric]??-1)-(a[metric]??-1)), total=state.totals[metric];$('rankings').replaceChildren();
  for(const r of rows){const b=document.createElement('button');b.className='rank-row';b.style.setProperty('--swatch',r.key==='unknown'?'#a6afbe':color(r.key));b.innerHTML=`<div class="rank-line"><span class="rank-name">${esc(display(r.key))}</span><span class="rank-number">${isCost?cost(r):short(r.total_tokens)} <span class="muted">· ${percent(percentage(r[metric],total))}</span></span></div><div class="rank-track"><div class="rank-fill" style="width:${total&&r[metric]!=null?100*r[metric]/total:0}%"></div></div>`;b.title=`Оценка API: ${cost(r)}; ${r.api_priced_events}/${r.events} событий с ценой. ${isCost?'Доля от рассчитанной стоимости. ':''}Вход без кэша: ${number(r.uncached_input_tokens)}; кэш / все: ${percent(r.cache_total_pct)}; reasoning / выход: ${percent(r.reasoning_output_pct)}`;b.onclick=()=>choose(key,r.key);$('rankings').append(b);}
  if(!rows.length)$('rankings').innerHTML='<div class="empty">Нет событий в выбранном срезе.</div>';
}
function tags(values){return values.map(v=>`<span class="tag ${v==='unknown'?'unknown':v==='fast'?'fast':''}">${esc(display(v))}</span>`).join('');}
function renderSessions(){
  $('sessions').replaceChildren();$('sessions-empty').hidden=state.sessions.length>0;
  $('session-count').textContent=`Показано ${state.sessions.length} из ${state.session_count}. Нажмите сессию для подробностей.`;
  for(const s of state.sessions){const tr=document.createElement('tr');tr.innerHTML=`<td><button class="session-open">${esc(s.machine)} · ${esc(s.key.slice(0,8))}</button><div class="subcell">${date(s.first_event_ms)} — ${date(s.last_event_ms)}</div></td><td>${esc(s.models.join(', '))}<div class="subcell">${tags(s.efforts)}${tags(s.tiers)}</div></td><td class="num">${number(s.total_tokens)}</td><td class="num" title="${s.api_priced_events}/${s.events} событий с ценой; ${s.api_assumed_tier_events} с допущением Standard">${cost(s)}</td><td class="num">${number(s.uncached_input_tokens)}</td><td class="num">${percent(s.cache_total_pct)}</td><td class="num">${percent(s.reasoning_output_pct)}</td><td class="num">${number(s.events)}</td>`;tr.querySelector('button').onclick=()=>openSession(s);$('sessions').append(tr);}
}
function svg(tag, attrs, text){const e=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v] of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
function tip(target,text){
  const show=e=>{const r=target.getBoundingClientRect(),x=e.clientX||r.x,y=e.clientY||r.y;$('tooltip').textContent=text;$('tooltip').style.left=Math.max(8,Math.min(x+12,innerWidth-295))+'px';$('tooltip').style.top=Math.max(8,Math.min(y+12,innerHeight-160))+'px';$('tooltip').hidden=false;};
  target.addEventListener('pointermove',show);target.addEventListener('pointerleave',()=>{$('tooltip').hidden=true;});target.addEventListener('focus',show);target.addEventListener('blur',()=>{$('tooltip').hidden=true;});
}
function drawChart(){
  if(!state)return;
  $('tooltip').hidden=true;
  const root=$('chart'),mode=$('chart-mode').value,metric=$('chart-metric').value;root.replaceChildren();$('legend').replaceChildren();$('chart-metric').disabled=mode==='tokens';
  const isCost=metric==='api_cost_known_usd', metricValue=v=>isCost?money(v):number(v);
  const rows=state.timeline, start=state.period.start_ms,end=state.period.end_ms,bucket=state.period.bucket_ms;
  $('chart-caption').textContent=(bucket===86400000?'По дням':bucket===3600000?'По часам':'По минутам')+' · значения при наведении · нажмите легенду, чтобы скрыть ряд'+(isCost&&mode!=='tokens'?' · API, рассчитанная часть':'' );
  if(!rows.length){root.innerHTML='<div class="empty">В этом срезе пока нет событий.<br>Измените период или фильтры.</div>';return;}
  const css=getComputedStyle(document.documentElement),swatch=n=>css.getPropertyValue(n).trim();
  const structures=[['uncached_input_tokens','Вход без кэша',swatch('--uncached')],['cached_input_tokens','Кэш',swatch('--cache')],['visible_output_tokens','Выход без reasoning',swatch('--visible')],['reasoning_output_tokens','Reasoning',swatch('--reasoning')]];
  let series=mode==='tokens'?structures:mode==='machines'?state.breakdowns.machine.map(m=>[m.key,m.key,color(m.key)]):[['current','Текущий интервал',swatch('--blue')],['previous','Предыдущий',swatch('--muted')]];
  if(mode==='tokens'&&rows.some(r=>structures.some(([k])=>r[k]==null))){root.innerHTML='<div class="empty">Не у всех событий есть полная разбивка.<br>Выберите график по машинам или тренд.</div>';return;}
  for(const [key,name,c] of series){const b=document.createElement('button');b.className=hiddenSeries.has(key)?'off':'';b.style.setProperty('--swatch',c);b.innerHTML=`<i></i>${esc(name)}`;b.setAttribute('aria-pressed',String(!hiddenSeries.has(key)));b.onclick=()=>{hiddenSeries.has(key)?hiddenSeries.delete(key):hiddenSeries.add(key);drawChart();};$('legend').append(b);}
  series=series.filter(([k])=>!hiddenSeries.has(k));
  const values=r=>series.map(([k])=>mode==='tokens'?r[k]:(r.machines.find(m=>m.key===k)||{})[metric]);
  const previous=state.previous_timeline||[];
  const samples=mode==='trend'?[...rows,...previous].map(r=>r[metric]).filter(v=>v!=null):rows.map(r=>values(r).reduce((s,v)=>s+(v??0),0));
  const max=Math.max(isCost&&mode!=='tokens'?.000001:1,...samples)*1.15,width=root.clientWidth,height=root.clientHeight,left=58,right=18,top=17,bottom=37,w=width-left-right,h=height-top-bottom;
  const x=t=>left+(t-start)/(end-start)*w,y=v=>top+h-v/max*h;
  const chart=svg('svg',{viewBox:`0 0 ${width} ${height}`,role:'img','aria-label':'Расход токенов во времени'});root.append(chart);
  for(let i=0;i<=4;i++){const v=max*i/4,yy=y(v);chart.append(svg('line',{x1:left,x2:width-right,y1:yy,y2:yy,stroke:'var(--line)'}));chart.append(svg('text',{x:left-8,y:yy+4,'text-anchor':'end'},isCost&&mode!=='tokens'?money(v):compact.format(v)));}
  for(let i=0;i<=3;i++){const t=start+(end-start)*i/3;chart.append(svg('text',{x:x(t),y:height-12,'text-anchor':i===0?'start':i===3?'end':'middle'},new Date(t).toLocaleString('ru-RU',bucket>=86400000?{day:'2-digit',month:'short'}:{hour:'2-digit',minute:'2-digit'})));}
  if(mode==='trend'){
    for(const [key,name,c] of series){const data=key==='current'?rows:previous,shift=key==='previous'?end-start:0;let path='',last=null;
      for(const r of data){const value=r[metric];if(value==null){last=null;continue;}const at=Math.min(end,Math.max(start,(r.timestamp_ms+(r.end_ms||r.timestamp_ms+bucket))/2+shift)),xx=x(at),yy=y(value);path+=(last!==null&&r.timestamp_ms-last<=bucket*1.1?' L':' M')+xx+' '+yy;last=r.timestamp_ms;const point=svg('circle',{cx:xx,cy:yy,r:4,fill:c,tabindex:0});tip(point,`${name}\n${date(r.timestamp_ms)}\n${metricValue(value)}${isCost?'':' токенов'}`);chart.append(point);}
      chart.prepend(svg('path',{d:path,fill:'none',stroke:c,'stroke-width':2,'stroke-dasharray':key==='previous'?'5 4':''}));
    }
  }else{
    for(const r of rows){let acc=0;const vals=values(r),bx=Math.max(left,x(r.timestamp_ms)),endx=Math.min(width-right,x(r.end_ms||r.timestamp_ms+bucket)),bw=Math.max(1,endx-bx-2);
      vals.forEach((v,i)=>{if(v==null||v===0)return;const rect=svg('rect',{x:bx,y:y(acc+v),width:bw,height:v/max*h,fill:series[i][2],rx:1,tabindex:0});tip(rect,`${date(r.timestamp_ms)}\n${series[i][1]}: ${mode==='tokens'?number(v):metricValue(v)}\nВсего в интервале: ${number(r.total_tokens)}`);chart.append(rect);acc+=v;});
    }
  }
}
async function refresh(){
  const id=++requestId;controller?.abort();controller=new AbortController();
  try{const query=params();const response=await fetch('/api/v1/analytics?'+query,{signal:controller.signal});if(response.status===401)throw Error('Требуется вход. Обновите страницу и введите логин и пароль сайта.');if(!response.ok)throw Error('Не удалось получить данные (HTTP '+response.status+').');const data=await response.json();if(id!==requestId)return;state=data;showError('');$('sync').textContent='Обновлено '+new Date().toLocaleTimeString('ru-RU');$('sync').classList.add('live');render();const url=new URLSearchParams();url.set('period',$('period').value);for(const key of dimensions)if($(key).value)url.set(key,$(key).value);if($('period').value==='custom'){url.set('from',$('from-date').value);url.set('to',$('to-date').value);}if(!$('zero-output').checked)url.set('zero_output','exclude');history.replaceState(null,'','?'+url);}
  catch(e){if(e.name==='AbortError')return;showError(e.message+' '+(state?'Показана последняя полученная выборка.':''));$('sync').textContent='Нет свежего обновления';$('sync').classList.remove('live');}
}
function choose(key,value){if(![...$(key).options].some(o=>o.value===value)){const o=document.createElement('option');o.value=value;o.textContent=display(value);$(key).append(o);}$(key).value=value;hiddenSeries.clear();refresh();}
async function openSession(session){
  selectedSession=session;$('detail-title').textContent=session.machine+' · '+session.key.slice(0,8);$('detail-subtitle').textContent=session.key;$('session-detail').textContent='Загрузка…';$('session-dialog').showModal();
  try{const q=params(true);q.set('machine',session.machine);q.set('session',session.key);const response=await fetch('/api/v1/analytics?'+q);if(!response.ok)throw Error('Не удалось загрузить сессию');const d=await response.json();if(selectedSession!==session)return;const t=d.totals;
    $('session-detail').innerHTML=`<div class="detail-metrics">${[['Все токены',short(t.total_tokens)],['Оценка API',cost(t)],['Вход без кэша',short(t.uncached_input_tokens)],['Кэш / все',percent(t.cache_total_pct)],['Reasoning / выход',percent(t.reasoning_output_pct)]].map(([label,value])=>`<div class="detail-metric"><span class="micro">${label}</span><strong>${value}</strong></div>`).join('')}</div><p class="micro">${number(t.events)} событий · среднее ${number(t.average_tokens)} токенов · ${date(t.first_event_ms)} — ${date(t.last_event_ms)}</p><div class="detail-section"><h3>Модели и режимы в этой сессии</h3>${['model','effort','tier'].map(k=>`<p>${d.breakdowns[k].map(r=>`${esc(display(r.key))}: <strong>${number(r.total_tokens)}</strong>`).join(' · ')}</p>`).join('')}</div><div class="detail-section"><h3>Последние ${d.recent_events.length} событий</h3><div class="table-wrap"><table><thead><tr><th>Время</th><th>Модель</th><th>Effort / скорость</th><th class="num">Вход без кэша</th><th class="num">Кэш</th><th class="num">Reasoning</th><th class="num">Всего</th></tr></thead><tbody>${d.recent_events.map(e=>`<tr><td>${date(e.timestamp_ms)}</td><td>${esc(e.model)}</td><td>${tags([e.effort,e.tier])}</td><td class="num">${number(e.uncached_input_tokens)}</td><td class="num">${number(e.cached_input_tokens)}</td><td class="num">${number(e.reasoning_output_tokens)}</td><td class="num">${number(e.total_tokens)}</td></tr>`).join('')}</tbody></table></div></div>`;
  }catch(e){$('session-detail').textContent=e.message;}
}
function download(blob,name){const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
$('csv-download').onclick=async()=>{if(!state)return;try{const r=await fetch('/api/v1/export.csv?'+params(true));if(!r.ok)throw Error('Ошибка выгрузки');download(await r.blob(),'codex-usage.csv');}catch(e){showError(e.message);}};
$('agent-open').onclick=async()=>{if(!state)return;$('agent-prompt').value='Проанализируй приложенную выгрузку Codex Usage. Объясни, какие машины, сессии, модели, effort и режимы fast дают основной расход. Сравни оценку стоимости API по машинам и моделям с учётом допущений pricing и покрытия api_priced_events. Сравни кэш от всех токенов и от входа, reasoning от выхода и от общей суммы, вход без кэша. Предложи 3 конкретных эксперимента для оптимизации с ожидаемым измеримым результатом. Используй semantics и coverage из файла: неизвестные режимы не заменяй стандартными, учитывай неполные интервалы. Не приравнивай токены к оплате или качеству и не делай причинных выводов только из сравнения разных задач. Ссылайся на конкретные числа и сессии.';$('agent-dialog').showModal();if(!schema){try{const r=await fetch('/api/v1/schema');if(r.ok)schema=await r.json();}catch{}}};
$('copy-prompt').onclick=async()=>{try{await navigator.clipboard.writeText($('agent-prompt').value);$('copy-prompt').textContent='Скопировано';}catch{$('agent-prompt').select();$('copy-prompt').textContent='Выделено — скопируйте текст';}};
$('json-download').onclick=()=>{if(state)download(new Blob([JSON.stringify({...state,semantics:schema?.semantics||state.semantics,analysis_prompt:$('agent-prompt').value},null,2)],{type:'application/json'}),'codex-analytics.json');};
for(const button of document.querySelectorAll('[data-close]'))button.onclick=()=>$(button.dataset.close).close();
for(const dialog of document.querySelectorAll('dialog'))dialog.addEventListener('click',e=>{if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)dialog.close();}});
$('period').onchange=()=>{$('custom-dates').hidden=$('period').value!=='custom';if($('period').value!=='custom')refresh();};$('apply-dates').onclick=refresh;
for(const key of dimensions)$(key).onchange=()=>{hiddenSeries.clear();refresh();};$('zero-output').onchange=refresh;
$('chart-mode').onchange=()=>{hiddenSeries.clear();drawChart();};$('chart-metric').onchange=drawChart;$('group-by').onchange=renderRankings;$('ranking-metric').onchange=renderRankings;
$('reset').onclick=()=>{for(const key of dimensions)$(key).value='';$('period').value='720';$('zero-output').checked=true;$('custom-dates').hidden=true;hiddenSeries.clear();refresh();};
const initial=new URLSearchParams(location.search);if([...$('period').options].some(o=>o.value===initial.get('period')))$('period').value=initial.get('period');for(const key of dimensions)if(initial.has(key)){const o=document.createElement('option');o.value=initial.get(key);o.textContent=display(o.value);$(key).append(o);$(key).value=o.value;}if(initial.get('from'))$('from-date').value=initial.get('from');if(initial.get('to'))$('to-date').value=initial.get('to');$('custom-dates').hidden=$('period').value!=='custom';$('zero-output').checked=initial.get('zero_output')!=='exclude';
new ResizeObserver(drawChart).observe($('chart'));refresh();setInterval(()=>{if(!document.hidden)refresh();},15000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});

function themeLabel(){const dark=document.documentElement.dataset.theme==='dark';$('theme-toggle').textContent=dark?'Светлая тема':'Тёмная тема';$('theme-toggle').setAttribute('aria-pressed',String(dark));}
$('theme-toggle').onclick=()=>{document.documentElement.dataset.theme=document.documentElement.dataset.theme==='dark'?'light':'dark';try{localStorage.setItem('codex-usage-theme',document.documentElement.dataset.theme);}catch{}themeLabel();if(state){renderRankings();drawChart();}};themeLabel();
$('pricing-open').onclick=async()=>{
  $('pricing-rates').textContent='Загрузка тарифов…';$('pricing-dialog').showModal();
  try{if(!schema){const r=await fetch('/api/v1/schema');if(!r.ok)throw Error('Не удалось получить тарифы');schema=await r.json();}
    $('pricing-rates').innerHTML='<table><thead><tr><th>Модель</th><th>Вход</th><th>Чтение кэша</th><th>Запись кэша</th><th>Выход</th></tr></thead><tbody>'+Object.entries(schema.pricing.rates_per_million).map(([model,card])=>'<tr><td>'+esc(model)+'</td>'+card.standard.map(v=>'<td>'+(v==null?'—':new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',minimumFractionDigits:2,maximumFractionDigits:6}).format(v))+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
  }catch(e){$('pricing-rates').textContent=e.message;}
};

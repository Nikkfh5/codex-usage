export default async function ({page}) {
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  const assert=(condition,message)=>{if(!condition)throw Error(message);};
  async function total(value){await page.waitForFunction(v=>document.getElementById('total').textContent.replace(/\s/g,'')===v,String(value),{timeout:12000});}
  await page.setViewportSize({width:1440,height:1100});
  await page.goto('http://127.0.0.1:8770/',{waitUntil:'networkidle',timeout:15000});await total(790);
  assert(await page.locator('.machine-card').count()===3,'Persistent machine cards missing');
  assert(await page.evaluate(()=>document.documentElement.dataset.theme)==='dark','Dark default missing');
  assert((await page.locator('#api-cost').textContent()).includes('$0.005772'),'Price formula or Fast rate wrong');
  await page.locator('#pricing-open').click();await page.waitForSelector('#pricing-rates table');
  assert((await page.locator('#pricing-rates').textContent()).includes('gpt-6-astra'),'Rate card missing');
  assert((await page.locator('#pricing-rates').textContent()).includes('$0.075'),'Fractional-cent rate was rounded');
  await page.locator('[data-close="pricing-dialog"]').click();
  await page.getByRole('button',{name:'Убрать карточку idle-host',exact:true}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.machine-card').length===2);await total(790);
  await page.reload({waitUntil:'networkidle'});await total(790);
  assert(await page.locator('.machine-card').count()===2,'Hidden card returned after reload');
  await page.locator('#hidden-count').click();await page.getByRole('button',{name:'Вернуть idle-host',exact:true}).click();
  await page.waitForFunction(()=>document.querySelectorAll('.machine-card').length===3);await total(790);
  await page.locator('#theme-toggle').click();
  assert(await page.evaluate(()=>document.documentElement.dataset.theme)==='light','Theme toggle failed');
  await page.reload({waitUntil:'networkidle'});await total(790);
  assert(await page.evaluate(()=>document.documentElement.dataset.theme)==='light','Theme preference lost');
  await page.locator('#theme-toggle').click();
  await page.locator('#chart-mode').selectOption('machines');await page.locator('#chart-metric').selectOption('api_cost_known_usd');
  assert(await page.locator('#chart svg').count()===1,'Dollar chart failed');
  await page.locator('#ranking-metric').selectOption('api_cost_known_usd');
  assert((await page.locator('#rankings').textContent()).includes('$'),'Dollar ranking missing');
  await page.locator('#ranking-metric').selectOption('total_tokens');await page.locator('#chart-metric').selectOption('total_tokens');await page.locator('#chart-mode').selectOption('tokens');

  assert((await page.locator('#cache-note').textContent()).includes('35,4% от всех'),'Cache/all denominator wrong');
  assert((await page.locator('#cache-note').textContent()).includes('40% от входа'),'Cache/input denominator wrong');
  await page.locator('#effort').selectOption('high');await total(350);
  await page.locator('#tier').selectOption('standard');await total('—');
  assert(await page.locator('#effort').inputValue()==='high','Empty slice reset effort');
  assert(await page.locator('#tier').inputValue()==='standard','Empty slice reset tier');
  assert(await page.locator('.machine-card').count()===3,'Empty slice lost machines');
  await page.locator('#effort').selectOption('');await total(340);
  await page.locator('#reset').click();await total(790);
  await page.locator('#zero-output').uncheck();await total(690);
  await page.locator('#zero-output').check();await total(790);
  await page.locator('#machine').selectOption('mac');await total(350);
  await page.locator('.session-open').first().click();
  await page.waitForSelector('#session-detail .detail-metric');
  assert((await page.locator('#session-detail').textContent()).includes('57,1%'),'Session ratios incorrect');
  await page.locator('[data-close="session-dialog"]').click();
  await page.locator('#reset').click();await total(790);
  for(const mode of ['machines','trend','tokens']){await page.locator('#chart-mode').selectOption(mode);assert(await page.locator('#chart svg').count()===1,'Chart mode failed: '+mode);}
  await page.locator('#legend button').first().click();assert(await page.locator('#legend button.off').count()===1,'Legend toggle failed');
  await page.locator('#legend button').first().click();
  await page.locator('#group-by').selectOption('tier');await page.locator('.rank-row').filter({hasText:'Fast'}).click();await total(350);
  await page.locator('#reset').click();await total(790);
  await page.locator('#period').selectOption('custom');await page.locator('#from-date').fill('2026-08-01');await page.locator('#to-date').fill('2026-08-02');await page.locator('#apply-dates').click();await total('—');
  assert(await page.locator('.machine-card').count()===3,'No persistent idle host list');
  await page.locator('#reset').click();await total(790);
  await page.locator('#agent-open').click();assert((await page.locator('#agent-prompt').inputValue()).includes('кэш от всех'),'Agent prompt incomplete');
  const downloadPromise=page.waitForEvent('download');await page.locator('#json-download').click();const download=await downloadPromise;assert(download.suggestedFilename()==='codex-analytics.json','JSON export failed');
  await page.locator('[data-close="agent-dialog"]').click();
  const csvPromise=page.waitForEvent('download');await page.locator('#csv-download').click();assert((await csvPromise).suggestedFilename()==='codex-usage.csv','CSV export failed');
  await page.screenshot({path:'/Users/nixito/codex-usage-lab/data/analytics-desktop-fixture.png',fullPage:true});
  for(const width of [390,320]){await page.setViewportSize({width,height:844});assert(!await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),'Mobile overflow '+width);}
  await page.setViewportSize({width:390,height:844});await page.screenshot({path:'/Users/nixito/codex-usage-lab/data/analytics-mobile-fixture.png',fullPage:true});
  assert(errors.length===0,errors.join('\n'));
  console.log(JSON.stringify({filters:'passed',weightedRatios:'passed',unknownModes:'passed',machineActivity:'passed',sessionDrilldown:'passed',chartModes:'passed',exports:'passed',pricing:'passed',hideRestore:'passed',themePersistence:'passed',mobile:'passed',errors}));
}

// Local preview on 8769, never the production receiver.
export default async function ({page}) {
  const errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.setViewportSize({width:1440,height:1100});
  await page.goto('http://127.0.0.1:8769/',{waitUntil:'networkidle',timeout:15000});
  await page.waitForFunction(()=>document.getElementById('sync').classList.contains('live'));
  const total=await page.locator('#total').textContent();
  for(const mode of ['machines','trend','tokens']) await page.locator('#chart-mode').selectOption(mode);
  if(await page.locator('.session-open').count()){
    await page.locator('.session-open').first().click();
    await page.waitForSelector('#session-detail .detail-metric');
    await page.locator('[data-close="session-dialog"]').click();
  }
  await page.screenshot({path:'/Users/nixito/codex-usage-lab/data/analytics-desktop-real.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});
  if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth))throw Error('Mobile overflow');
  await page.screenshot({path:'/Users/nixito/codex-usage-lab/data/analytics-mobile-real.png',fullPage:true});
  if(errors.length)throw Error(errors.join('\n'));
  console.log(JSON.stringify({observedTotal:total,desktop:'1440',mobile:'390',errors}));
}

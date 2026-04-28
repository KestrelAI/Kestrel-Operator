function fixCtaButton() {
  var btn = document.getElementById('topbar-cta-button');
  if (!btn) return;

  var bg = getComputedStyle(document.body).backgroundColor;
  var match = bg.match(/\d+/g);
  if (!match) return;

  var r = parseInt(match[0]), g = parseInt(match[1]), b = parseInt(match[2]);
  var luminance = (0.299 * r + 0.587 * g + 0.114 * b);
  var isDark = luminance < 128;

  if (isDark) {
    btn.style.setProperty('background-color', '#FAFAFA', 'important');
    btn.style.setProperty('color', '#09090B', 'important');
  }
  btn.style.setProperty('border-color', 'transparent', 'important');
}

fixCtaButton();
setInterval(fixCtaButton, 500);
new MutationObserver(fixCtaButton).observe(document.documentElement, { attributes: true });

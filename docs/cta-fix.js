// Fix CTA button in dark mode
function fixCtaButton() {
  var btn = document.getElementById('topbar-cta-button');
  if (!btn) return;
  
  var isDark = document.documentElement.classList.contains('dark') ||
               document.documentElement.getAttribute('data-theme') === 'dark' ||
               document.documentElement.style.colorScheme === 'dark' ||
               window.matchMedia('(prefers-color-scheme: dark)').matches;
  
  if (isDark) {
    btn.style.setProperty('background-color', '#FAFAFA', 'important');
    btn.style.setProperty('color', '#09090B', 'important');
    btn.style.setProperty('border-color', 'transparent', 'important');
  } else {
    btn.style.setProperty('border-color', 'transparent', 'important');
  }
}

// Run on load and observe theme changes
fixCtaButton();
setInterval(fixCtaButton, 500);

var observer = new MutationObserver(fixCtaButton);
observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class', 'data-theme', 'style'] });

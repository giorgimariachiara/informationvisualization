function toRoman(num) {
  var roman = "";
  var romanNumerals = ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"];
  var numbers = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1];

  for (var i = 0; i < numbers.length; i++) {
    while (num >= numbers[i]) {
      roman += romanNumerals[i];
      num -= numbers[i];
    }
  }

  return roman;
}

$(document).ready(function() {
  var container = $('#flipbook');
  var containerWidth = container.width();
  var containerHeight = container.height();

  // Verifica se il contenitore ha altezza e larghezza specificate
  if (containerWidth === 0 || containerHeight === 0 || isNaN(containerWidth) || isNaN(containerHeight)) {
    // Assegna altezza e larghezza predefinite al contenitore
    container.width(window.innerWidth);
    container.height(window.innerHeight);
  }

  var homePage = 0; // Imposta la pagina iniziale come home (index.html)

  var pages = [
    'pagina1.html',
    'pagina2.html',
    'pagina3.html',
    // Aggiungi altre pagine HTML qui
  ];

  function updateNavbar(page) {
    navbar.find('.nav-button').removeClass('active');
    navbar.find('.nav-button[data-page="' + page + '"]').addClass('active');
  }

  var navbar = $('<div class="navbar">');
  container.append(navbar);

  for (var i = 0; i < pages.length; i++) {
    var pageNum = i + 1;
    var romanNumeral = toRoman(pageNum);
    var pageButton = $('<div class="nav-button">' + romanNumeral + '</div>');
    pageButton.data('page', pageNum);
    pageButton.on('click', function() {
      var pageNum = $(this).data('page');
      // Carica la pagina HTML corrispondente
      $('#content').load(pages[pageNum - 1]);
      updateNavbar(pageNum);
    });
    navbar.append(pageButton);
  }

  navbar.css('bottom', '10px');

  var prevButton = $('<button class="nav-button">Previous</button>');
  prevButton.on('click', function() {
    var activePage = navbar.find('.nav-button.active');
    var prevPage = activePage.data('page') - 1;
    if (prevPage >= 1) {
      activePage.removeClass('active');
      activePage.prev().addClass('active');
      // Carica la pagina HTML precedente
      $('#content').load(pages[prevPage - 1]);
    }
  });
  navbar.append(prevButton);

  var nextButton = $('<button class="nav-button">Next</button>');
  nextButton.on('click', function() {
    var activePage = navbar.find('.nav-button.active');
    var nextPage = activePage.data('page') + 1;
    if (nextPage <= pages.length) {
      activePage.removeClass('active');
      activePage.next().addClass('active');
      // Carica la pagina HTML successiva
      $('#content').load(pages[nextPage - 1]);
    }
  });
  navbar.append(nextButton);

  updateNavbar(homePage);
});

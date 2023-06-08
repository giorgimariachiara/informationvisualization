import 'flipbook';
import 'jquery';

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

$(document).ready(function($) {
  var container = $('#flipbook');
  var containerWidth = container.width();
  var containerHeight = container.height();

  // Verifica se il contenitore ha altezza e larghezza specificate
  if (containerWidth == 0) {
    // Handle case when containerWidth is 0
  } else if (containerHeight == 0) {
    // Handle case when containerHeight is 0
  } else if (isNaN(containerWidth)) {
    // Handle case when containerWidth is NaN
  } else if (isNaN(containerHeight)) {
    // Handle case when containerHeight is NaN
  } else {
    // Code to execute when none of the conditions are met
    container.width(window.innerWidth);
    container.height(window.innerHeight);
  }

  var homePage = 0; // Imposta la pagina iniziale come home (index.html)

  var pages = [
    'index.html',
    'pagina1.html',
    'pagina2.html',
    'pagina3.html',
    // Elenca gli altri file HTML per le pagine successive
  ];

  container.FlipBook({
    width: '100%',
    height: '100%',
    startPage: homePage,
    pages: pages,
    pageNumbers: true,
    gradients: true,
    autoCenter: true,
    turnCorners: true,
    acceleration: true,
    onPageChange: function(event, page) {
      console.log('Turned to page:', page);
      updateNavbar(page);
    },
    onFullscreenError: function(message) {
      // Handle fullscreen error
    }
  });

  var navbar = $('<div class="navbar">');
  container.append(navbar);

  function updateNavbar(page) {
    navbar.find('.nav-button').removeClass('active');
    navbar.find('.nav-button[data-page="' + page + '"]').addClass('active');
  }

  for (var i = 0; i < pages.length; i++) {
    var pageNum = i + 1;
    var romanNumeral = toRoman(pageNum);
    var pageButton = $('<div class="nav-button">' + romanNumeral + '</div>');
    pageButton.data('page', pageNum);
    pageButton.on('click', function() {
      var pageNum = $(this).data('page');
      container.FlipBook('page', pageNum);
    });
    navbar.append(pageButton);
  }

  navbar.css('bottom', '10px');

  var prevButton = $('<button class="nav-button">Previous</button>');
  prevButton.on('click', function() {
    container.FlipBook('previous');
  });
  navbar.append(prevButton);

  var nextButton = $('<button class="nav-button">Next</button>');
  nextButton.on('click', function() {
    container.FlipBook('next');
  });
  navbar.append(nextButton);

  updateNavbar(homePage); // Imposta la home come attiva all'avvio
});

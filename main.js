
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
window.onload = function() {
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
          'index.html', 
          'pagina1.html',
          'pagina2.html',
          'pagina3.html',
          // Elenca gli altri file HTML per le pagine successive
        ];

        container.turn({
          width: '100%',
          height: '100%',
          display: 'single',
          autoCenter: true,
          turnCorners: true,
          gradients: true,
          acceleration: true,
          pages: pages.length,
          when: {
            turning: function(event, page, view) {
              console.log('Turning page:', page);
            },
            turned: function(event, page, view) {
              console.log('Turned to page:', page);
              updateNavbar(page);
            },
            first: function(event) {
              container.turn('page', homePage);
            }
          },
          page: function(event, page) {
            var pageURL = pages[page - 1];
            container.turn('page', page, pageURL);
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
            container.turn('page', pageNum);
          });
          navbar.append(pageButton);
        }

        navbar.css('bottom', '10px');

        var prevButton = $('<button class="nav-button">Previous</button>');
        prevButton.on('click', function() {
          container.turn('previous');
        });
        navbar.append(prevButton);

        var nextButton = $('<button class="nav-button">Next</button>');
        nextButton.on('click', function() {
          container.turn('next');
        });
        navbar.append(nextButton);

        updateNavbar(homePage); // Imposta la home come attiva all'avvio
      });
}	


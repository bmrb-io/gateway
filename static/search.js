    // Used to escape regular expressions characters in the text that is
    //  selected for the autofill
    function escapeRegExp(to_escape){
        return (to_escape+'').replace(/([.*+?^${}()|\[\]\/\\])/g, "\\$1").replace(/\n/g, "\\n");
    };

    // Use this to highlight the query words in a given text
    function highlight_words(words, text){
        regex = new RegExp("(" + words.join("|") + ")", "ig");
        return text.replace(regex, '<strong>$1</strong>');
    }

    // These two are for the instant search - at top of page. Do not remove!
    function add_color_span_instant(ul, item, id) {

        var terms = document.getElementById(id).value.split(/[ ,]+/);
        var display = highlight_words(terms, item.db + " " + item.entry + ": " + item.termname + "=" + item.term);

        return $("<li></li>")
            .data("item.autocomplete", item)
            .append("<a><span style='cursor:pointer;'>" + display + "</span></a>")
            .appendTo(ul);
    }

    $( "#search" ).autocomplete({
        minLength: 2,
        delay: 0,
        source: "/search/query",
        select: function( event, ui ) {
            // Don't put the value in the box when they click
            event.preventDefault();
            // Jump to the entry summary page when they click
            window.location.href = "/" + ui.item.link;
            return false;
        }
    }).off('blur').on('blur', function() {
        if(document.hasFocus()) {
            $('ul.ui-autocomplete').hide();
        }
    }).data("ui-autocomplete")._renderItem = function (ul, item) {
         return add_color_span_instant(ul, item, "search");
    };

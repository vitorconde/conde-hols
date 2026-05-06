// ****
// To be placed in assets/javascripts/username-replacer.js
// ****
(function() {
    const PLACEHOLDERS = {
        USERNAME: 'USERNAME',
        DB_NAME: 'DB_NAME'
    };
    const STORAGE_KEYS = {
        USER_NUMBER: 'mkdocs_user_number'
    };

    // Helper: Recursively walk through DOM nodes to find and replace text within TextNodes only.
    // This ensures we don't break surrounding span tags used for syntax highlighting.
    function walkAndReplace(node, replacements) {
        if (node.nodeType === Node.TEXT_NODE) {
            // Replace all placeholders in this text node
            let updatedText = node.nodeValue;
            Object.entries(replacements).forEach(([key, value]) => {
                if (value && value.trim() !== '') {
                    const regex = new RegExp(PLACEHOLDERS[key], 'g');
                    updatedText = updatedText.replace(regex, value);
                }
            });
            node.nodeValue = updatedText;
        } else if (node.nodeType === Node.ELEMENT_NODE) {
            // If it's an element (like a syntax highlight span), explore its children
            Array.from(node.childNodes).forEach(child => {
                walkAndReplace(child, replacements);
            });
        }
    }

    // 1. Initialization: Find blocks containing the placeholders and store their original HTML structure.
    function initCodeBlocks() {
        // We target all <code> elements (both block-level in <pre> and inline)
        const codeBlocks = document.querySelectorAll('code');

        codeBlocks.forEach(block => {
            // Check if the raw text contains any placeholder before we do expensive DOM work
            const hasPlaceholder = Object.values(PLACEHOLDERS).some(placeholder => 
                block.textContent.includes(placeholder)
            );
            
            if (hasPlaceholder && !block.hasAttribute('data-original-html')) {
                // IMPORTANT: We save innerHTML now, not textContent, to preserve syntax highlighting spans.
                block.setAttribute('data-original-html', block.innerHTML);
            }
        });
    }

    // 2. The Replacement Logic
    function updateCodeBlocks(replacements) {
        const targetBlocks = document.querySelectorAll('code[data-original-html]');

        targetBlocks.forEach(block => {
            // A. Always RESET to the original HTML structure first.
            // This brings back the placeholders and all original syntax highlighting spans.
            block.innerHTML = block.getAttribute('data-original-html');

            // B. If we have valid replacements, perform the surgical replacement
            walkAndReplace(block, replacements);
        });
    }

    // 3. Handle User Input (UI listeners)
    function setupEventListeners() {
        const dropdown = document.getElementById('user-number-input');
        const saveButton = document.getElementById('user-username-save');
        const clearButton = document.getElementById('user-username-clear');
        const selectedDisplay = document.getElementById('selected-username');

        if (!dropdown || !saveButton) return;

        // Load saved value from localStorage
        const savedNumber = localStorage.getItem(STORAGE_KEYS.USER_NUMBER);
        
        if (savedNumber) {
            dropdown.value = savedNumber;
            if (selectedDisplay) {
                selectedDisplay.textContent = savedNumber;
            }
        }

        saveButton.addEventListener('click', function() {
            const userNumber = dropdown.value.trim();
            
            // Auto-generate username and database name
            // Extract numbers from userNumber (e.g., "user001" -> "001")
            const numberPart = userNumber.replace(/\D/g, '');
            const username = userNumber ? userNumber : '';
            const dbName = numberPart ? `cdc_db${numberPart}` : '';
            
            // Update the display
            if (selectedDisplay) {
                selectedDisplay.textContent = userNumber || 'None';
            }
            
            // Save to localStorage
            localStorage.setItem(STORAGE_KEYS.USER_NUMBER, userNumber);
            
            // Update code blocks with replacements
            updateCodeBlocks({
                USERNAME: username,
                DB_NAME: dbName
            });
        });

        if(clearButton) {
            clearButton.addEventListener('click', function() {
                localStorage.removeItem(STORAGE_KEYS.USER_NUMBER);
                dropdown.value = "";
                // Reset display
                if (selectedDisplay) {
                    selectedDisplay.textContent = 'None';
                }
                // Reset all placeholders
                updateCodeBlocks({
                    USERNAME: '',
                    DB_NAME: ''
                });
            });
        }
    }


    // 4. Main Execution and navigation handling
    function run() {
        initCodeBlocks();
        setupEventListeners();
        
        // Load saved value and apply it
        const savedNumber = localStorage.getItem(STORAGE_KEYS.USER_NUMBER) || '';
        // Extract numbers from savedNumber (e.g., "user001" -> "001")
        const numberPart = savedNumber.replace(/\D/g, '');
        const username = savedNumber ? savedNumber : '';
        const dbName = numberPart ? `cdc_db${numberPart}` : '';
        
        updateCodeBlocks({
            USERNAME: username,
            DB_NAME: dbName
        });
    }

    // Initial load
    document.addEventListener("DOMContentLoaded", run);

    // Handle navigation in Material theme SPA mode
    if (window.location$) {
        window.location$.subscribe(function() {
            // A slight delay ensures the new page content is loaded into the DOM
            setTimeout(run, 100);
        });
    }

})();
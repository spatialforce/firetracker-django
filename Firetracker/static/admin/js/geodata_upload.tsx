document.addEventListener('DOMContentLoaded', function() {
    function updateFormatField(select) {
        const formatSelect = document.getElementById('id_upload_format');
        if (!formatSelect) return;
        
        const selectedType = select.value;
        formatSelect.innerHTML = '';
        
        const options = {
            'firepoint': ['csv'],
            'province': ['json', 'shp'],
            'district': ['json', 'shp']
        };
        
        options[selectedType].forEach(function(format) {
            const option = document.createElement('option');
            option.value = format;
            option.textContent = format.toUpperCase();
            formatSelect.appendChild(option);
        });
        
        // Toggle auxiliary files field
        const auxiliaryField = document.querySelector('.field-auxiliary_files');
        if (auxiliaryField) {
            auxiliaryField.style.display = 
                formatSelect.value === 'shp' ? 'block' : 'none';
        }
    }
    
    // Initialize on page load
    const typeSelect = document.getElementById('id_data_type');
    if (typeSelect) {
        typeSelect.addEventListener('change', function() {
            updateFormatField(this);
        });
        updateFormatField(typeSelect);
    }
    
    // Handle format change
    const formatSelect = document.getElementById('id_upload_format');
    if (formatSelect) {
        formatSelect.addEventListener('change', function() {
            const auxiliaryField = document.querySelector('.field-auxiliary_files');
            if (auxiliaryField) {
                auxiliaryField.style.display = 
                    this.value === 'shp' ? 'block' : 'none';
            }
        });
    }
});
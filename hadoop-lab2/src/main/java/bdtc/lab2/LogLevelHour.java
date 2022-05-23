package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LogLevelHour {

    private long hour;
    // Уровень логирования
    private String aeroport1;
    private String aeroport2;
    // Час, в который произошло событие
}

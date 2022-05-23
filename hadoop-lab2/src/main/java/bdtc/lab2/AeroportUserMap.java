package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class AeroportUserMap {
    private String aeroport;
    private String country;
}

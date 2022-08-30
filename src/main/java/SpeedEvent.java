import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SpeedEvent {
    private int speed;
    private String latitude;
    private String longitude;
    private String terminal_phone;
}

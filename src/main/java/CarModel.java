
import lombok.*;

/**
 * Car is a POJO class which contains information about the car.
 */
@Builder
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor(force = true)
public final class CarModel {

    final String Name;
    final String Chevrolet;
    final String Ford;
    final  String Infiniti;
    final String Suzuki;
    final  String TATA;
    final  String Toyota;
    final  String Acura;

}
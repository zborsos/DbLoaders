package local.utils;

import java.util.Random;

public final class random {
    public static Boolean getRandoTrueFalse(){
        Random random = new Random();
        return random.nextBoolean();
    }
}
